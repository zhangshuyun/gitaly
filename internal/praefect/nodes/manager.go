package nodes

import (
	"context"
	"errors"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Shard is a primary with a set of secondaries
type Shard interface {
	GetPrimary() (Node, error)
	GetSecondaries() ([]Node, error)
}

// Manager is responsible for returning shards for virtual storages
type Manager interface {
	GetShard(virtualStorageName string) (Shard, error)
}

// Node represents some metadata of a node as well as a connection
type Node interface {
	GetStorage() string
	GetAddress() string
	GetToken() string
	GetConnection() *grpc.ClientConn
}

type shard struct {
	m           sync.RWMutex
	primary     *nodeStatus
	secondaries []*nodeStatus
}

// GetPrimary gets the primary of a shard
func (s *shard) GetPrimary() (Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.primary, nil
}

// GetSecondaries gets the secondaries of a shard
func (s *shard) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var secondaries []Node
	for _, secondary := range s.secondaries {
		secondaries = append(secondaries, secondary)
	}

	return secondaries, nil
}

// Mgr is a concrete type that adheres to the Manager interface
type Mgr struct {
	// shards is a map of shards keyed on virtual storage name
	shards map[string]*shard
	// staticShards never changes based on node health. It is a static set of shards that comes from the config's
	// VirtualStorages
	failoverEnabled bool
	log             *logrus.Entry
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

// NewNodeManager creates a new NodeMgr based on virtual storage configs
func NewManager(log *logrus.Entry, c config.Config, dialOpts ...grpc.DialOption) (*Mgr, error) {
	shards := make(map[string]*shard)
	for _, virtualStorage := range c.VirtualStorages {
		var secondaries []*nodeStatus
		var primary *nodeStatus
		for _, node := range virtualStorage.Nodes {
			conn, err := client.Dial(node.Address,
				append(
					[]grpc.DialOption{
						grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
						grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(node.Token)),
						grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
						grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
					}, dialOpts...),
			)
			if err != nil {
				return nil, err
			}
			ns := newConnectionStatus(*node, conn, log)

			if node.DefaultPrimary {
				primary = ns
				continue
			}

			secondaries = append(secondaries, ns)
		}

		shards[virtualStorage.Name] = &shard{
			primary:     primary,
			secondaries: secondaries,
		}
	}

	return &Mgr{
		shards:          shards,
		log:             log,
		failoverEnabled: c.FailoverEnabled,
	}, nil
}

// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
// for deeming a node "healthy"
const healthcheckThreshold = 3

func (n *Mgr) bootstrap(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C
		n.checkShards()
		timer.Reset(d)
	}

	return nil
}

func (n *Mgr) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		<-ticker.C
		n.checkShards()
	}
}

// Start will bootstrap the node manager by calling healthcheck on the nodes as well as kicking off
// the monitoring process. Start must be called before NodeMgr can be used.
func (n *Mgr) Start(bootstrapInterval, monitorInterval time.Duration) {
	if n.failoverEnabled {
		n.bootstrap(bootstrapInterval)
		go n.monitor(monitorInterval)
	}
}

var ErrVirtualStorageNotExist = errors.New("virtual storage does not exist")

// GetShard retrieves a shard for a virtual storage name
func (n *Mgr) GetShard(virtualStorageName string) (Shard, error) {
	shard, ok := n.shards[virtualStorageName]
	if !ok {
		return nil, ErrVirtualStorageNotExist
	}

	if n.failoverEnabled {
		if !shard.primary.isHealthy() {
			return nil, ErrPrimaryNotHealthy
		}
	}

	return shard, nil
}

func checkShard(virtualStorage string, s *shard) {
	defer func() {
		metrics.PrimaryGauge.WithLabelValues(virtualStorage, s.primary.GetStorage()).Set(1)
		for _, secondary := range s.secondaries {
			metrics.PrimaryGauge.WithLabelValues(virtualStorage, secondary.GetStorage()).Set(0)
		}
	}()

	s.primary.check()
	for _, secondary := range s.secondaries {
		secondary.check()
	}

	if s.primary.isHealthy() {
		return
	}

	newPrimaryIndex := -1
	for i, secondary := range s.secondaries {
		if secondary.isHealthy() {
			newPrimaryIndex = i
			break
		}
	}

	if newPrimaryIndex < 0 {
		// no healthy secondaries exist
		return
	}
	s.m.Lock()
	newPrimary := s.secondaries[newPrimaryIndex]
	s.secondaries = append(s.secondaries[:newPrimaryIndex], s.secondaries[newPrimaryIndex+1:]...)
	s.secondaries = append(s.secondaries, s.primary)
	s.primary = newPrimary
	s.m.Unlock()
}

func (n *Mgr) checkShards() {
	for virtualStorage, shard := range n.shards {
		checkShard(virtualStorage, shard)
	}
}

func newConnectionStatus(node models.Node, cc *grpc.ClientConn, l *logrus.Entry) *nodeStatus {
	return &nodeStatus{
		Node:       node,
		ClientConn: cc,
		statuses:   make([]healthpb.HealthCheckResponse_ServingStatus, 0),
		log:        l,
	}
}

type nodeStatus struct {
	models.Node
	*grpc.ClientConn
	statuses []healthpb.HealthCheckResponse_ServingStatus
	log      *logrus.Entry
}

// GetStorage gets the storage name of a node
func (n *nodeStatus) GetStorage() string {
	return n.Storage
}

// GetAddress gets the address of a node
func (n *nodeStatus) GetAddress() string {
	return n.Address
}

// GetToken gets the token of a node
func (n *nodeStatus) GetToken() string {
	return n.Token
}

// GetConnection gets the client connection of a node
func (n *nodeStatus) GetConnection() *grpc.ClientConn {
	return n.ClientConn
}

func (n *nodeStatus) isHealthy() bool {
	if len(n.statuses) < healthcheckThreshold {
		return false
	}

	for _, status := range n.statuses[len(n.statuses)-healthcheckThreshold:] {
		if status != healthpb.HealthCheckResponse_SERVING {
			return false
		}
	}

	return true
}

func (n *nodeStatus) check() {
	client := healthpb.NewHealthClient(n.ClientConn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		n.log.WithError(err).WithField("address", n.Address).Warn("error when pinging healthcheck")
		resp = &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_UNKNOWN,
		}
	}

	n.statuses = append(n.statuses, resp.Status)
	if len(n.statuses) > healthcheckThreshold {
		n.statuses = n.statuses[1:]
	}
}
