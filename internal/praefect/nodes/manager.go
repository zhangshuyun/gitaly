package nodes

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/hashicorp/consul/api"
	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
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
	shards map[string]*shard
	// staticShards never changes based on node health. It is a static set of shards that comes from the config's
	// VirtualStorages
	failoverEnabled bool
	log             *logrus.Entry
	consulClient    *api.Client
}

// ErrPrimaryNotHealthy indicates the primary of a shard is not in a healthy state and hence
// should not be used for a new request
var ErrPrimaryNotHealthy = errors.New("primary is not healthy")

// NewNodeManager creates a new NodeMgr based on virtual storage configs
func NewManager(log *logrus.Entry, c config.Config, dialOpts ...grpc.DialOption) (*Mgr, error) {
	consulClient, err := api.NewClient(&api.Config{
		Address: c.ConsulAddress,
		Scheme:  "http",
	})

	if err != nil {
		return nil, err
	}

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
		consulClient:    consulClient,
	}, nil
}

func (n *Mgr) Start() {
	ticker := time.NewTicker(3 * time.Second)
	tries := 5
	for {
		<-ticker.C
		tries--
		if tries == 0 {
			n.log.Error("maximum number of tries reached when registering nodes")
		}
		err := n.registerNodes()
		if err != nil {
			n.log.WithError(err).Warn("error when registering nodes")
			continue
		}

		return
	}
}

func (n *Mgr) registerNodes() error {
	var g errgroup.Group

	for virtualStorageName, shard := range n.shards {
		for _, node := range append(shard.secondaries, shard.primary) {
			node := node
			g.Go(func() error {
				return n.registerConsulAndPoll(virtualStorageName, node.Storage, node.Address, node.GetConnection())
			})
		}
	}

	return g.Wait()
}

func (n *Mgr) getPrimary(virtualStorageName string) (*Value, error) {
	kv, _, err := n.consulClient.KV().Get(getKey(virtualStorageName), nil)
	if err != nil {
		return nil, err
	}

	var value Value

	if kv == nil || kv.Value == nil {
		return nil, errors.New("no primary has been elected")
	}

	if err := json.Unmarshal(kv.Value, &value); err != nil {
		return nil, err
	}
	return &value, nil
}

// GetShard retrieves a shard for a virtual storage name
func (n *Mgr) GetShard(virtualStorageName string) (Shard, error) {
	shard, ok := n.shards[virtualStorageName]
	if !ok {
		return nil, errors.New("virtual storage does not exist")
	}

	if !n.failoverEnabled {
		panic("no failover")
		return shard, nil
	}

	leader, err := n.getPrimary(virtualStorageName)
	if err != nil {
		return nil, err
	}

	nodes := append(shard.secondaries, shard.primary)

	var secondaries []*nodeStatus

	for _, node := range nodes {
		if node.Storage == leader.Storage {
			shard.primary = node
			continue
		}
		secondaries = append(secondaries, node)
	}

	shard.secondaries = secondaries

	return shard, nil
}

func (n *Mgr) checkShards() {
	for _, shard := range n.shards {
		shard.m.Lock()

		shard.m.Unlock()
	}
}

func newConnectionStatus(node models.Node, cc *grpc.ClientConn, l *logrus.Entry) *nodeStatus {
	return &nodeStatus{
		Node:       node,
		ClientConn: cc,
		log:        l,
	}
}

type nodeStatus struct {
	models.Node
	*grpc.ClientConn
	log *logrus.Entry
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

func (n *Mgr) registerConsulAndPoll(virtualStorageName, internalStorageName, address string, cc *grpc.ClientConn) error {
	key := getKey(virtualStorageName)
	value, err := getValue(internalStorageName)
	if err != nil {
		return err
	}

	checkID, err := n.registerConsul(internalStorageName, key, value, address)
	if err != nil {
		return err
	}

	go n.keepTryingToBePrimary(virtualStorageName, internalStorageName, key, value, checkID)

	return nil
}

func getValue(internalStorageName string) ([]byte, error) {
	b, err := json.Marshal(&Value{Storage: internalStorageName})

	if err != nil {
		return nil, err
	}

	return b, nil
}

func getKey(virtualStorageName string) string {
	return fmt.Sprintf("service/%s/primary", virtualStorageName)
}

func (n *Mgr) registerConsul(nodeName string, key string, value []byte, address string) (string, error) {
	addressWithoutScheme := strings.TrimPrefix(address, "tcp://")
	checkID := fmt.Sprintf("gitaly-internal-%s", addressWithoutScheme)

	agent := n.consulClient.Agent()
	if err := agent.ServiceRegister(&api.AgentServiceRegistration{
		Address: addressWithoutScheme,
		ID:      addressWithoutScheme,
		Name:    nodeName,
		Tags:    []string{"monitoring"},
		Check: &api.AgentServiceCheck{
			CheckID:  checkID,
			Name:     "gitaly internal grpc health",
			GRPC:     addressWithoutScheme,
			Interval: "1s",
		},
	}); err != nil {
		return "", err
	}

	return checkID, nil
}

type Value struct {
	Storage string `json:"storage"`
}

func (n *Mgr) checkHealth(serviceName string) error {
	checks, _, err := n.consulClient.Health().Checks(serviceName, nil)
	if err != nil {
		return err
	}
	if status := checks.AggregatedStatus(); status != "passing" {
		return fmt.Errorf("service %s unhealthy: %s", serviceName, status)
	}
	return nil
}

func (n *Mgr) becomePrimary(serviceName string, checkID string, key string, b []byte) {
	sessionID, _, err := n.consulClient.Session().Create(&api.SessionEntry{
		// Sessions are scoped to a node. This session is for an internal Gitaly
		// service. The session should be scoped to the node that internal
		// service is running on.
		//	Node:          internalNodeName,
		Behavior:      "delete",
		ServiceChecks: []api.ServiceCheck{api.ServiceCheck{ID: checkID}},
	}, nil)
	if err != nil {
		n.log.WithField("nodeName", serviceName).WithError(err).Error("couldn't get session")
		return
	}

	isLeader := false
	defer func() {
		if !isLeader {
			if _, err := n.consulClient.Session().Destroy(sessionID, nil); err != nil {
				n.log.WithField("nodeName", serviceName).WithError(err).Error("couldn't destroy session")
			}
		}
	}()

	isLeader, _, err = n.consulClient.KV().Acquire(&api.KVPair{
		Key:     key, // distributed lock
		Value:   b,
		Session: sessionID,
	}, nil)
	if err != nil {
		n.log.WithField("nodeName", serviceName).WithField("session", sessionID).WithError(err).Error("couldn't update leader key")
		return
	}

	if isLeader {
		n.log.WithField("nodeName", serviceName).Info("I'm the new leader 😈!")
	} else {
		n.log.WithField("nodeName", serviceName).Info("failed to become leader")
	}
}

func (n *Mgr) keepTryingToBePrimary(virtualStorageName, serviceName string, key string, b []byte, checkID string) {
	for ; ; time.Sleep(1 * time.Second) {
		if _, err := n.getPrimary(virtualStorageName); err == nil {
			// no failover required
			continue
		}

		if err := n.checkHealth(serviceName); err != nil {
			n.log.WithField("nodeName", serviceName).WithError(err).Error("node is not healthy or health check failed")
			continue
		}

		n.becomePrimary(serviceName, checkID, key, b)
	}
}
