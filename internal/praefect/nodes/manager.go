package nodes

import (
	"context"
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

// GetShard retrieves a shard for a virtual storage name
func (n *Mgr) GetShard(virtualStorageName string) (Shard, error) {
	shard, ok := n.shards[virtualStorageName]
	if !ok {
		return nil, errors.New("virtual storage does not exist")
	}

	if !n.failoverEnabled {
		return shard, nil
	}

	key := getKey(virtualStorageName)

	kv, _, err := n.consulClient.KV().Get(key, nil)
	if err != nil {
		logrus.WithError(err).Error("error when getting leader")
		return nil, err
	}

	var leader Value

	if kv == nil || kv.Value == nil {
		return nil, errors.New("no primary has been elected")
	}

	if err := json.Unmarshal(kv.Value, &leader); err != nil {
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
	isLeader, sessionID, err := n.registerConsul(key, value, address)
	if err != nil {
		return err
	}

	go n.keepTryingToBePrimary(cc, key, sessionID, isLeader, value)

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

func (n *Mgr) registerConsul(key string, value []byte, address string) (bool, string, error) {
	addressWithoutScheme := strings.TrimPrefix(address, "tcp://")
	if err := n.consulClient.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Address: addressWithoutScheme,
		ID:      addressWithoutScheme,
		Name:    "monitoring",
		Tags:    []string{"monitoring"},
		Check: &api.AgentServiceCheck{
			Name:     "Service health status",
			GRPC:     addressWithoutScheme,
			Interval: "10s",
		},
	}); err != nil {
		return false, "", err
	}

	sessionID, _, err := n.consulClient.Session().Create(&api.SessionEntry{
		Name:     key, // distributed lock
		Behavior: "delete",
		TTL:      "10s",
	}, nil)

	isLeader, _, err := n.consulClient.KV().Acquire(&api.KVPair{
		Key:     key, // distributed lock
		Value:   value,
		Session: sessionID,
	}, nil)

	if err != nil {
		return false, "", err
	}

	if isLeader {
		n.log.WithField("internal_storage", address).Info("I am the leader!! 👑")
	}

	return isLeader, sessionID, nil
}

type Value struct {
	Storage string `json:"storage"`
}

func (n *Mgr) keepTryingToBePrimary(cc *grpc.ClientConn, key, sessionID string, isLeader bool, b []byte) {
	doneChan := make(chan struct{})

	if isLeader {
		go n.consulClient.Session().RenewPeriodic(
			"10s",
			sessionID,
			nil,
			doneChan,
		)
	}

	for {
		<-time.Tick(5 * time.Second)
		var err error

		client := healthpb.NewHealthClient(cc)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		if err != nil {
			n.log.WithError(err).WithField("storage", string(b)).Error("error when pinging healthcheck")
		}
		if err != nil || resp.Status != healthpb.HealthCheckResponse_SERVING {
			if isLeader {
				n.log.Error("CLOSING CHANNEL")
				close(doneChan)
				isLeader = false
			}
			continue
		}

		if !isLeader {
			n.log.WithField("value", string(b)).Info("I'm not the primary but I really want to be")

			sessionID, _, err := n.consulClient.Session().Create(&api.SessionEntry{
				Name:     key, // distributed lock
				Behavior: "delete",
				TTL:      "10s",
			}, nil)
			if err != nil {
				n.log.WithField("value", string(b)).WithError(err).Error("couldn't get session")
				continue
			}

			isLeader, _, err = n.consulClient.KV().Acquire(&api.KVPair{
				Key:     key, // distributed lock
				Value:   b,
				Session: sessionID,
			}, nil)

			if isLeader {
				n.log.WithField("value", string(b)).Info("I'm the new leader 😈!")
				doneChan = make(chan struct{})

				go n.consulClient.Session().RenewPeriodic(
					"10s",
					sessionID,
					nil,
					doneChan,
				)
			}
		}
	}
}
