package nodes

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type nodeCandidate struct {
	m        sync.RWMutex
	node     Node
	statuses []bool
}

// localElector relies on an in-memory datastore to track the primary
// and secondaries. A single election strategy pertains to a single
// shard. It does NOT support multiple Praefect nodes or have any
// persistence. This is used mostly for testing and development.
type localElector struct {
	m                     sync.RWMutex
	failoverEnabled       bool
	shardName             string
	nodes                 []*nodeCandidate
	primaryNode           *nodeCandidate
	readOnlyAfterFailover bool
	isReadOnly            bool
	log                   logrus.FieldLogger
}

// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
// for deeming a node "healthy"
const healthcheckThreshold = 3

func (n *nodeCandidate) checkNode(ctx context.Context) {
	status, _ := n.node.check(ctx)

	n.m.Lock()
	defer n.m.Unlock()

	n.statuses = append(n.statuses, status)

	if len(n.statuses) > healthcheckThreshold {
		n.statuses = n.statuses[1:]
	}
}

func (n *nodeCandidate) isHealthy() bool {
	n.m.RLock()
	defer n.m.RUnlock()

	if len(n.statuses) < healthcheckThreshold {
		return false
	}

	for _, status := range n.statuses[len(n.statuses)-healthcheckThreshold:] {
		if !status {
			return false
		}
	}

	return true
}

func newLocalElector(name string, failoverEnabled, readOnlyAfterFailover bool, log logrus.FieldLogger, ns []*nodeStatus) *localElector {
	nodes := make([]*nodeCandidate, len(ns))
	for i, n := range ns {
		nodes[i] = &nodeCandidate{
			node: n,
		}
	}

	return &localElector{
		shardName:             name,
		failoverEnabled:       failoverEnabled,
		log:                   log,
		nodes:                 nodes,
		primaryNode:           nodes[0],
		readOnlyAfterFailover: readOnlyAfterFailover,
	}
}

// Start launches a Goroutine to check the state of the nodes and
// continuously monitor their health via gRPC health checks.
func (s *localElector) start(bootstrapInterval, monitorInterval time.Duration) {
	s.bootstrap(bootstrapInterval)
	go s.monitor(monitorInterval)
}

func (s *localElector) bootstrap(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C

		ctx := context.TODO()
		s.checkNodes(ctx)
		timer.Reset(d)
	}
}

func (s *localElector) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	ctx := context.Background()

	for {
		<-ticker.C

		err := s.checkNodes(ctx)

		if err != nil {
			s.log.WithError(err).Warn("error checking nodes")
		}
	}
}

func (*localElector) Describe(chan<- *prometheus.Desc) {
	// ignored in the example
}

func (*localElector) Collect(chan<- prometheus.Metric) {
	// ignored in the example.
	//
	// If you look at the implementation of sqlElector.Collect, we can
	// see that it would be straightforward to extract the instrumentation
	// code in to a wrapper that wraps a leaderElectionStrategy and instruments
	// the calls to GetShard and uses GetShard to fetch the current primaries
	// and secondaries, reducing code duplication between the local and the sql
	// elector
}

// checkNodes issues a gRPC health check for each node managed by the
// shard.
func (s *localElector) checkNodes(ctx context.Context) error {
	for _, n := range s.nodes {
		n.checkNode(ctx)
	}

	s.m.Lock()
	defer s.m.Unlock()

	if s.primaryNode != nil && s.primaryNode.isHealthy() {
		return nil
	}

	var newPrimary *nodeCandidate

	for _, node := range s.nodes {
		if node != s.primaryNode && node.isHealthy() {
			newPrimary = node
			break
		}
	}

	if newPrimary == nil {
		return ErrPrimaryNotHealthy
	}

	s.primaryNode = newPrimary
	s.isReadOnly = s.readOnlyAfterFailover

	return nil
}

// GetShard gets the current status of the shard. If primary is not elected
// or it is unhealthy and failover is enabled, ErrPrimaryNotHealthy is
// returned.
func (s *localElector) GetShard() (Shard, error) {
	s.m.RLock()
	primary := s.primaryNode
	isReadOnly := s.isReadOnly
	s.m.RUnlock()

	if primary == nil {
		return Shard{}, ErrPrimaryNotHealthy
	}

	if s.failoverEnabled && !primary.isHealthy() {
		return Shard{}, ErrPrimaryNotHealthy
	}

	var secondaries []Node
	for _, n := range s.nodes {
		if n != primary {
			secondaries = append(secondaries, n.node)
		}
	}

	return Shard{
		IsReadOnly:  isReadOnly,
		Primary:     primary.node,
		Secondaries: secondaries,
	}, nil
}

func (s *localElector) enableWrites(context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.primaryNode == nil || !s.primaryNode.isHealthy() {
		return ErrPrimaryNotHealthy
	}

	s.isReadOnly = false
	return nil
}
