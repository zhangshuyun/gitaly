package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	prommetrics "gitlab.com/gitlab-org/gitaly/internal/prometheus/metrics"
	correlation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Shard is a primary with a set of secondaries
type Shard struct {
	// PreviousWritablePrimary is the virtual storage's previous
	// write-enabled primary.
	PreviousWritablePrimary Node
	IsReadOnly              bool
	Primary                 Node
	Secondaries             []Node
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
	GetShard(virtualStorageName string) (Shard, error)
	// EnableWrites enables writes for a given virtual storage. Returns an
	// ErrPrimaryNotHealthy if the shard does not have a healthy primary.
	// ErrVirtualStorageNotExist if a virtual storage with the given name
	// does not exist.
	EnableWrites(ctx context.Context, virtualStorageName string) error
	// GetSyncedNode returns a random storage node based on the state of the replication.
	// It returns primary in case there are no up to date secondaries or error occurs.
	GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (Node, error)
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
	failoverEnabled bool
	// log must be used only for non-request specific needs like bootstrapping, etc.
	// for request related logging `ctxlogrus.Extract(ctx)` must be used.
	log *logrus.Entry
	// strategies is a map of strategies keyed on virtual storage name
	strategies map[string]leaderElectionStrategy
	db         *sql.DB
	queue      datastore.ReplicationEventQueue
}

// leaderElectionStrategy defines the interface by which primary and
// secondaries are managed.
type leaderElectionStrategy interface {
	start(bootstrapInterval, monitorInterval time.Duration)
	checkNodes(context.Context) error
	GetShard() (Shard, error)
	enableWrites(context.Context) error
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

const dialTimeout = 10 * time.Second

// NewManager creates a new NodeMgr based on virtual storage configs
func NewManager(
	log *logrus.Entry,
	c config.Config,
	db *sql.DB,
	queue datastore.ReplicationEventQueue,
	latencyHistogram prommetrics.HistogramVec,
	registry *protoregistry.Registry,
	errorTracker tracker.ErrorTracker,
	dialOpts ...grpc.DialOption) (*Mgr, error) {
	strategies := make(map[string]leaderElectionStrategy, len(c.VirtualStorages))

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	for _, virtualStorage := range c.VirtualStorages {
		log = log.WithField("virtual_storage", virtualStorage.Name)

		ns := make([]*nodeStatus, 0, len(virtualStorage.Nodes))
		for _, node := range virtualStorage.Nodes {
			streamInterceptors := []grpc.StreamClientInterceptor{
				grpc_prometheus.StreamClientInterceptor,
				grpctracing.StreamClientTracingInterceptor(),
				correlation.StreamClientCorrelationInterceptor(),
			}

			if c.Failover.Enabled && errorTracker != nil {
				streamInterceptors = append(streamInterceptors, middleware.StreamErrorHandler(registry, errorTracker, node.Storage))
			}

			conn, err := client.DialContext(ctx, node.Address,
				append([]grpc.DialOption{
					grpc.WithBlock(),
					grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
					grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(node.Token)),
					grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(streamInterceptors...)),
				}, dialOpts...),
			)
			if err != nil {
				return nil, err
			}
			cs := newConnectionStatus(*node, conn, log, latencyHistogram, errorTracker)
			ns = append(ns, cs)
		}

		if c.Failover.Enabled && c.Failover.ElectionStrategy == "sql" {
			strategies[virtualStorage.Name] = newSQLElector(virtualStorage.Name, c, db, log, ns)
		} else {
			strategies[virtualStorage.Name] = newLocalElector(virtualStorage.Name, c.Failover.Enabled, c.Failover.ReadOnlyAfterFailover, log, ns)
		}
	}

	return &Mgr{
		log:             log,
		db:              db,
		failoverEnabled: c.Failover.Enabled,
		strategies:      strategies,
		queue:           queue,
	}, nil
}

// Start will bootstrap the node manager by calling healthcheck on the nodes as well as kicking off
// the monitoring process. Start must be called before NodeMgr can be used.
func (n *Mgr) Start(bootstrapInterval, monitorInterval time.Duration) {
	if n.failoverEnabled {
		n.log.Info("Starting failover checks")

		for _, strategy := range n.strategies {
			strategy.start(bootstrapInterval, monitorInterval)
		}
	} else {
		n.log.Info("Failover checks are disabled")
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
func (n *Mgr) GetShard(virtualStorageName string) (Shard, error) {
	strategy, ok := n.strategies[virtualStorageName]
	if !ok {
		return Shard{}, fmt.Errorf("virtual storage %q: %w", virtualStorageName, ErrVirtualStorageNotExist)
	}

	shard, err := strategy.GetShard()
	if err != nil {
		return shard, err
	}

	return shard, nil
}

func (n *Mgr) EnableWrites(ctx context.Context, virtualStorageName string) error {
	strategy, ok := n.strategies[virtualStorageName]
	if !ok {
		return ErrVirtualStorageNotExist
	}

	return strategy.enableWrites(ctx)
}

func (n *Mgr) GetSyncedNode(ctx context.Context, virtualStorageName, repoPath string) (Node, error) {
	shard, err := n.GetShard(virtualStorageName)
	if err != nil {
		return nil, fmt.Errorf("get shard for %q: %w", virtualStorageName, err)
	}

	if !featureflag.IsEnabled(ctx, featureflag.DistributedReads) {
		return shard.Primary, nil
	}

	logger := ctxlogrus.Extract(ctx).WithFields(logrus.Fields{"virtual_storage_name": virtualStorageName, "repo_path": repoPath})
	upToDateStorages, err := n.queue.GetUpToDateStorages(ctx, virtualStorageName, repoPath)
	if err != nil {
		// this is recoverable error - proceed with primary node
		logger.WithError(err).Warn("get up to date secondaries")
	}

	// Make sure that nodes are unique in case the up-to-date storages also
	// contain the primary.
	storages := make(map[Node]struct{}, len(upToDateStorages)+1) // +1 is for the primary node
	storages[shard.Primary] = struct{}{}

	for _, upToDateStorage := range upToDateStorages {
		node, err := shard.GetNode(upToDateStorage)
		if err != nil {
			// this is recoverable error - proceed with with other nodes
			logger.WithError(err).Warn("storage returned as up-to-date")
		}

		if !node.IsHealthy() {
			continue
		}

		storages[node] = struct{}{}
	}

	i := rand.Intn(len(storages))
	for storage := range storages {
		if i == 0 {
			return storage, nil
		}
		i--
	}

	return nil, errors.New("could not select random storage")
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
	if n.errTracker != nil {
		if n.errTracker.ReadThresholdReached(n.GetStorage()) {
			n.updateStatus(false)
			return false, fmt.Errorf("read error threshold reached for storage %q", n.GetStorage())
		}

		if n.errTracker.WriteThresholdReached(n.GetStorage()) {
			n.updateStatus(false)
			return false, fmt.Errorf("write error threshold reached for storage %q", n.GetStorage())
		}
	}

	health := healthpb.NewHealthClient(n.clientConn)
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	status := false

	start := time.Now()
	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	n.latencyHist.WithLabelValues(n.node.Storage).Observe(time.Since(start).Seconds())

	if err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING {
		status = true
	} else {
		n.log.WithError(err).WithFields(logrus.Fields{
			"storage": n.node.Storage,
			"address": n.node.Address,
		}).Warn("error when pinging healthcheck")
	}

	var gaugeValue float64
	if status {
		gaugeValue = 1
	}
	metrics.NodeLastHealthcheckGauge.WithLabelValues(n.GetStorage()).Set(gaugeValue)

	n.updateStatus(status)

	return status, err
}
