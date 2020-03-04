package nodes

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
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
	m        sync.RWMutex
	primary  *nodeStatus
	allNodes []*nodeStatus
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
	for _, node := range s.allNodes {
		if s.primary != node {
			secondaries = append(secondaries, node)
		}
	}

	return secondaries, nil
}

// Mgr is a concrete type that adheres to the Manager interface
type Mgr struct {
	shards map[string]*shard
	// staticShards never changes based on node health. It is a static set of shards that comes from the config's
	// VirtualStorages
	failoverEnabled bool
	log             *logrus.Entry
	db              *sql.DB
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

// NewManager creates a new NodeMgr based on virtual storage configs
func NewManager(log *logrus.Entry, c config.Config, dialOpts ...grpc.DialOption) (*Mgr, error) {
	db, err := glsql.OpenDB(c.DB)
	if err != nil {
		return nil, err
	}

	shards := make(map[string]*shard)
	for _, virtualStorage := range c.VirtualStorages {
		var primary *nodeStatus
		var nodes []*nodeStatus
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

			nodes = append(nodes, ns)

			if node.DefaultPrimary {
				primary = ns
			}
		}

		shards[virtualStorage.Name] = &shard{
			primary:  primary,
			allNodes: nodes,
		}
	}

	return &Mgr{
		shards:          shards,
		log:             log,
		failoverEnabled: c.FailoverEnabled,
		db:              db,
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
		n.log.Info("starting health checks!")

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

// GetShard retrieves a shard for a virtual storage name
func (n *Mgr) GetShard(virtualStorageName string) (Shard, error) {
	shard, ok := n.shards[virtualStorageName]
	if !ok {
		return nil, errors.New("virtual storage does not exist")
	}

	if n.failoverEnabled {
		//		if !shard.primary.isHealthy() {
		//			return nil, ErrPrimaryNotHealthy
		//		}
	}

	return shard, nil
}

func (n *Mgr) updateLeader(shardName string, storageName string) {
	n.db.Exec("INSERT INTO shard_elections (is_primary, shard_name, node_name, last_seen_active) " +
		"VALUES ('t', " + "'" + shardName + "'," + "'" + storageName + "', now()) ON CONFLICT (is_primary, shard_name) " +
		"DO UPDATE SET " +
		"node_name = " +
		"CASE WHEN (shard_elections.last_seen_active < now() - interval '20 seconds') THEN " +
		"shard_elections.node_name " +
		"ELSE " +
		"excluded.node_name " +
		"END, " +
		"last_seen_active = " +
		"CASE WHEN (shard_elections.last_seen_active < now() - interval '20 seconds') THEN " +
		"	 shard_elections.last_seen_active " +
		"ELSE " +
		"excluded.last_seen_active " +
		"END")
}

func (n *Mgr) LookupPrimary(shardName string) (*nodeStatus, error) {
	rows, err := n.db.Query("SELECT node_name FROM shard_elections WHERE is_primary IS TRUE AND shard_name = '" + shardName + "'")

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		shard := n.shards[shardName]

		for _, node := range shard.allNodes {
			if node.GetStorage() == name {
				return node, nil
			}
		}
	}

	return nil, err
}

func (n *Mgr) checkShards() {
	for shardName, shard := range n.shards {
		for _, node := range shard.allNodes {
			if node.check() {
				n.log.Info("health check good for " + node.GetStorage())
				n.updateLeader(shardName, node.GetStorage())
			} else {
				n.log.Info("health check failed for " + node.GetStorage())
			}
		}

		primary, err := n.LookupPrimary(shardName)

		if err != nil {
			shard.m.Lock()
			shard.primary = primary
			shard.m.Unlock()
		}
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

func (n *nodeStatus) check() bool {
	client := healthpb.NewHealthClient(n.ClientConn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	success := false
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		n.log.WithError(err).WithField("address", n.Address).Warn("error when pinging healthcheck")
		resp = &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_UNKNOWN,
		}
	} else {
		success = resp.Status == healthpb.HealthCheckResponse_SERVING
	}

	n.statuses = append(n.statuses, resp.Status)
	if len(n.statuses) > healthcheckThreshold {
		n.statuses = n.statuses[1:]
	}

	return success
}
