package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	prommetrics "gitlab.com/gitlab-org/gitaly/internal/prometheus/metrics"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Shard is a primary with a set of secondaries
type Shard struct {
	Primary     Node
	Secondaries []Node
}

func (s Shard) GetNode(storage string) (Node, error) {
	if storage == s.Primary.GetStorage() {
		return s.Primary, nil
	}

	for _, node := range s.Secondaries {
		if storage == node.GetStorage() {
			return node, nil
		}
	}

	return nil, fmt.Errorf("node with storage %q does not exist", storage)
}

// GetHealthySecondaries returns all secondaries of the shard whose which are
// currently known to be healthy.
func (s Shard) GetHealthySecondaries() []Node {
	healthySecondaries := make([]Node, 0, len(s.Secondaries))
	for _, secondary := range s.Secondaries {
		if !secondary.IsHealthy() {
			continue
		}
		healthySecondaries = append(healthySecondaries, secondary)
	}
	return healthySecondaries
}

// Manager is responsible for returning shards for virtual storages
type Manager interface {
	GetShard(ctx context.Context, virtualStorageName string) (Shard, error)
	// GetSyncedNode returns a random storage node based on the state of the replication.
	// It returns primary in case there are no up to date secondaries or error occurs.
	GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (Node, error)
	// HealthyNodes returns healthy storages by virtual storage.
	HealthyNodes() map[string][]string
	// Nodes returns nodes by their virtual storages.
	Nodes() map[string][]Node
}

const (
	// healthcheckTimeout is the max duration allowed for checking of node health status.
	// If check takes more time it considered as failed.
	healthcheckTimeout = 1 * time.Second
	// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
	// for deeming a node "healthy"
	healthcheckThreshold = 3
)

// Node represents some metadata of a node as well as a connection
type Node interface {
	GetStorage() string
	GetAddress() string
	GetToken() string
	GetConnection() *grpc.ClientConn
	// IsHealthy reports if node is healthy and can handle requests.
	// Node considered healthy if last 'healthcheckThreshold' checks were positive.
	IsHealthy() bool
	// CheckHealth executes health check for the node and tracks last 'healthcheckThreshold' checks for it.
	CheckHealth(context.Context) (bool, error)
}

// Mgr is a concrete type that adheres to the Manager interface
type Mgr struct {
	// strategies is a map of strategies keyed on virtual storage name
	strategies map[string]leaderElectionStrategy
	db         *sql.DB
	// nodes contains nodes by their virtual storages
	nodes      map[string][]Node
	csg        datastore.ConsistentStoragesGetter
	muxedConns map[string]muxedNodes
}

// leaderElectionStrategy defines the interface by which primary and
// secondaries are managed.
type leaderElectionStrategy interface {
	start(bootstrapInterval, monitorInterval time.Duration)
	checkNodes(context.Context) error
	GetShard(ctx context.Context) (Shard, error)
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

const dialTimeout = 10 * time.Second

// Dial dials a node with the necessary interceptors configured.
func Dial(ctx context.Context, node *config.Node, registry *protoregistry.Registry, errorTracker tracker.ErrorTracker, handshaker client.Handshaker) (*grpc.ClientConn, error) {
	streamInterceptors := []grpc.StreamClientInterceptor{
		grpc_prometheus.StreamClientInterceptor,
	}

	if errorTracker != nil {
		streamInterceptors = append(streamInterceptors, middleware.StreamErrorHandler(registry, errorTracker, node.Storage))
	}

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(node.Token)),
		grpc.WithChainStreamInterceptor(streamInterceptors...),
		grpc.WithChainUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
	}

	return client.Dial(ctx, node.Address, dialOpts, handshaker)
}

type muxedNodes map[string]*grpc.ClientConn

type muxedNode struct {
	Node
	muxedConn *grpc.ClientConn
}

func (n muxedNode) GetConnection() *grpc.ClientConn { return n.muxedConn }

func (mn muxedNodes) getNode(ctx context.Context, node Node) Node {
	if featureflag.IsDisabled(ctx, featureflag.ConnectionMultiplexing) {
		return node
	}

	muxedConn, ok := mn[node.GetStorage()]
	if !ok {
		ctxlogrus.Extract(ctx).WithField("storage", node.GetStorage()).Error("no multiplexed connection to Gitaly")
		return node
	}

	return muxedNode{Node: node, muxedConn: muxedConn}
}

// NewManager creates a new NodeMgr based on virtual storage configs
func NewManager(
	log *logrus.Entry,
	c config.Config,
	db *sql.DB,
	csg datastore.ConsistentStoragesGetter,
	latencyHistogram prommetrics.HistogramVec,
	registry *protoregistry.Registry,
	errorTracker tracker.ErrorTracker,
	handshaker client.Handshaker,
) (*Mgr, error) {
	if !c.Failover.Enabled {
		errorTracker = nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	nodes := make(map[string][]Node, len(c.VirtualStorages))
	strategies := make(map[string]leaderElectionStrategy, len(c.VirtualStorages))
	muxed := map[string]muxedNodes{}
	for _, virtualStorage := range c.VirtualStorages {
		log = log.WithField("virtual_storage", virtualStorage.Name)

		ns := make([]*nodeStatus, 0, len(virtualStorage.Nodes))
		vsMuxed := muxedNodes{}
		for _, node := range virtualStorage.Nodes {
			conn, err := Dial(ctx, node, registry, errorTracker, nil)
			if err != nil {
				return nil, err
			}

			cs := newConnectionStatus(*node, conn, log, latencyHistogram, errorTracker)
			ns = append(ns, cs)

			if c.Failover.ElectionStrategy != config.ElectionStrategySQL {
				continue
			}

			muxedConn, err := Dial(ctx, node, registry, errorTracker, handshaker)
			if err != nil {
				log.WithError(err).Error("failed to dial Gitaly over a muxed connection")
				continue
			}

			vsMuxed[cs.GetStorage()] = muxedConn
		}

		for _, node := range ns {
			nodes[virtualStorage.Name] = append(nodes[virtualStorage.Name], node)
		}

		if c.Failover.Enabled {
			if c.Failover.ElectionStrategy == config.ElectionStrategySQL {
				strategies[virtualStorage.Name] = newSQLElector(virtualStorage.Name, c, db, log, ns, vsMuxed)
				muxed[virtualStorage.Name] = vsMuxed
			} else {
				strategies[virtualStorage.Name] = newLocalElector(virtualStorage.Name, log, ns)
			}
		} else {
			strategies[virtualStorage.Name] = newDisabledElector(virtualStorage.Name, ns)
		}
	}

	return &Mgr{
		db:         db,
		strategies: strategies,
		nodes:      nodes,
		muxedConns: muxed,
		csg:        csg,
	}, nil
}

// Start will bootstrap the node manager by calling healthcheck on the nodes as well as kicking off
// the monitoring process. Start must be called before NodeMgr can be used.
func (n *Mgr) Start(bootstrapInterval, monitorInterval time.Duration) {
	for _, strategy := range n.strategies {
		strategy.start(bootstrapInterval, monitorInterval)
	}
}

// checkShards performs health checks on all the available shards. The
// election strategy is responsible for determining the criteria for
// when to elect a new primary and when a node is down.
func (n *Mgr) checkShards() {
	for _, strategy := range n.strategies {
		ctx := context.Background()
		strategy.checkNodes(ctx)
	}
}

// ErrVirtualStorageNotExist indicates the node manager is not aware of the virtual storage for which a shard is being requested
var ErrVirtualStorageNotExist = errors.New("virtual storage does not exist")

// GetShard retrieves a shard for a virtual storage name
func (n *Mgr) GetShard(ctx context.Context, virtualStorageName string) (Shard, error) {
	strategy, ok := n.strategies[virtualStorageName]
	if !ok {
		return Shard{}, fmt.Errorf("virtual storage %q: %w", virtualStorageName, ErrVirtualStorageNotExist)
	}

	return strategy.GetShard(ctx)
}

// GetPrimary returns the current primary of a repository. This is an adapter so NodeManager can be used
// as a praefect.PrimaryGetter in newer code which written to support repository specific primaries.
func (n *Mgr) GetPrimary(ctx context.Context, virtualStorage, _ string) (string, error) {
	shard, err := n.GetShard(ctx, virtualStorage)
	if err != nil {
		return "", err
	}

	return shard.Primary.GetStorage(), nil
}

func (n *Mgr) GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (Node, error) {
	upToDateStorages, err := n.csg.GetConsistentStorages(ctx, virtualStorageName, repoPath)
	if err != nil && !errors.As(err, new(commonerr.RepositoryNotFoundError)) {
		return nil, err
	}

	if len(upToDateStorages) == 0 {
		// this possible when there is no data yet in the database for the repository
		shard, err := n.GetShard(ctx, virtualStorageName)
		if err != nil {
			return nil, fmt.Errorf("get shard for %q: %w", virtualStorageName, err)
		}

		upToDateStorages = map[string]struct{}{shard.Primary.GetStorage(): {}}
	}

	healthyStorages := make([]Node, 0, len(upToDateStorages))
	nodes := n.getNodes(ctx, virtualStorageName)
	for _, node := range nodes {
		if !node.IsHealthy() {
			continue
		}

		if _, ok := upToDateStorages[node.GetStorage()]; !ok {
			continue
		}

		healthyStorages = append(healthyStorages, node)
	}

	if len(healthyStorages) == 0 {
		return nil, fmt.Errorf("no healthy nodes: %w", ErrPrimaryNotHealthy)
	}

	return healthyStorages[rand.Intn(len(healthyStorages))], nil
}

func (n *Mgr) HealthyNodes() map[string][]string {
	healthy := make(map[string][]string, len(n.nodes))
	for vs, nodes := range n.nodes {
		storages := make([]string, 0, len(nodes))
		for _, node := range nodes {
			if node.IsHealthy() {
				storages = append(storages, node.GetStorage())
			}
		}

		healthy[vs] = storages
	}

	return healthy
}

func (n *Mgr) Nodes() map[string][]Node { return n.nodes }

func (n *Mgr) getNodes(ctx context.Context, virtualStorage string) []Node {
	nodes := make([]Node, 0, len(n.nodes[virtualStorage]))
	for _, node := range n.nodes[virtualStorage] {
		nodes = append(nodes, n.muxedConns[virtualStorage].getNode(ctx, node))
	}

	return nodes
}

func newConnectionStatus(node config.Node, cc *grpc.ClientConn, l logrus.FieldLogger, latencyHist prommetrics.HistogramVec, errorTracker tracker.ErrorTracker) *nodeStatus {
	return &nodeStatus{
		node:        node,
		clientConn:  cc,
		log:         l,
		latencyHist: latencyHist,
		errTracker:  errorTracker,
	}
}

type nodeStatus struct {
	node        config.Node
	clientConn  *grpc.ClientConn
	log         logrus.FieldLogger
	latencyHist prommetrics.HistogramVec
	mtx         sync.RWMutex
	statuses    []bool
	errTracker  tracker.ErrorTracker
}

// GetStorage gets the storage name of a node
func (n *nodeStatus) GetStorage() string {
	return n.node.Storage
}

// GetAddress gets the address of a node
func (n *nodeStatus) GetAddress() string {
	return n.node.Address
}

// GetToken gets the token of a node
func (n *nodeStatus) GetToken() string {
	return n.node.Token
}

// GetConnection gets the client connection of a node
func (n *nodeStatus) GetConnection() *grpc.ClientConn {
	return n.clientConn
}

func (n *nodeStatus) IsHealthy() bool {
	n.mtx.RLock()
	healthy := n.isHealthy()
	n.mtx.RUnlock()
	return healthy
}

func (n *nodeStatus) isHealthy() bool {
	if len(n.statuses) < healthcheckThreshold {
		return false
	}

	for _, ok := range n.statuses[len(n.statuses)-healthcheckThreshold:] {
		if !ok {
			return false
		}
	}

	return true
}

func (n *nodeStatus) updateStatus(status bool) {
	n.mtx.Lock()
	n.statuses = append(n.statuses, status)
	if len(n.statuses) > healthcheckThreshold {
		n.statuses = n.statuses[1:]
	}
	n.mtx.Unlock()
}

func (n *nodeStatus) CheckHealth(ctx context.Context) (bool, error) {
	health := healthpb.NewHealthClient(n.clientConn)
	if n.errTracker != nil {
		health = tracker.NewHealthClient(health, n.GetStorage(), n.errTracker)
	}

	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()

	start := time.Now()
	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
	n.latencyHist.WithLabelValues(n.node.Storage).Observe(time.Since(start).Seconds())
	if err != nil {
		n.log.WithError(err).WithFields(logrus.Fields{
			"storage": n.node.Storage,
			"address": n.node.Address,
		}).Warn("error when pinging healthcheck")
	}

	status := resp.GetStatus() == healthpb.HealthCheckResponse_SERVING

	metrics.NodeLastHealthcheckGauge.WithLabelValues(n.GetStorage()).Set(metrics.BoolAsFloat(status))

	n.updateStatus(status)

	return status, err
}
