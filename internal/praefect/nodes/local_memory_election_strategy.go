package nodes

import (
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
)

type managedNode struct {
	node     Node
	up       bool
	primary  bool
	statuses []bool
}

// LocalMemoryElectionStrategy relies on an in-memory datastore to track
// the primary and secondaries. It does NOT support multiple Praefect
// nodes or have any persistence. This is used mostly for testing and
// development.
type LocalMemoryElectionStrategy struct {
	m               sync.RWMutex
	failoverEnabled bool
	shardName       string
	nodes           []*managedNode
	primaryNode     *managedNode
}

// healthcheckThreshold is the number of consecutive healthpb.HealthCheckResponse_SERVING necessary
// for deeming a node "healthy"
const healthcheckThreshold = 3

func (n *managedNode) isHealthy() bool {
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

func newLocalMemoryElectionStrategy(name string, failoverEnabled bool) *LocalMemoryElectionStrategy {
	return &LocalMemoryElectionStrategy{
		shardName: name,
	}
}

// AddNode registers a primary or secondary in the internal
// datastore.
func (s *LocalMemoryElectionStrategy) AddNode(node Node, primary bool) {
	localNode := managedNode{
		node:     node,
		primary:  primary,
		statuses: make([]bool, 0),
		up:       false,
	}

	// If failover hasn't been activated, we assume all nodes are up
	// since health checks aren't run.
	if !s.failoverEnabled {
		localNode.up = true
	}

	s.m.Lock()
	defer s.m.Unlock()

	if primary {
		s.primaryNode = &localNode
	}

	s.nodes = append(s.nodes, &localNode)
}

// Start launches a Goroutine to check the state of the nodes and
// continuously monitor their health via gRPC health checks.
func (s *LocalMemoryElectionStrategy) Start(bootstrapInterval, monitorInterval time.Duration) error {
	s.bootstrap(bootstrapInterval)
	go s.monitor(monitorInterval)

	return nil
}

func (s *LocalMemoryElectionStrategy) bootstrap(d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C
		s.CheckShard()
		timer.Reset(d)
	}

	return nil
}

func (s *LocalMemoryElectionStrategy) monitor(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		<-ticker.C
		s.CheckShard()
	}
}

// CheckShard issues a gRPC health check for each node managed by the
// shard.
func (s *LocalMemoryElectionStrategy) CheckShard() {
	defer s.updateMetrics()

	for _, n := range s.nodes {
		status := n.node.check()
		n.statuses = append(n.statuses, status)

		if len(n.statuses) > healthcheckThreshold {
			n.statuses = n.statuses[1:]
		}

		up := n.isHealthy()
		n.up = up
	}

	if s.primaryNode != nil && s.primaryNode.isHealthy() {
		return
	}

	var newPrimary *managedNode

	for _, node := range s.nodes {
		if !node.primary && node.isHealthy() {
			newPrimary = node
			break
		}
	}

	if newPrimary == nil {
		return
	}

	s.m.Lock()
	defer s.m.Unlock()

	s.primaryNode.primary = false
	s.primaryNode = newPrimary
	newPrimary.primary = true
}

// GetPrimary gets the primary of a shard. If no primary exists, it will
// be nil. If a primary has been elected but is down, err will be
// ErrPrimaryNotHealthy.
func (s *LocalMemoryElectionStrategy) GetPrimary() (Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.primaryNode == nil {
		return nil, ErrPrimaryNotHealthy
	}

	if !s.primaryNode.up {
		return s.primaryNode.node, ErrPrimaryNotHealthy
	}

	return s.primaryNode.node, nil
}

// GetSecondaries gets the secondaries of a shard
func (s *LocalMemoryElectionStrategy) GetSecondaries() ([]Node, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var secondaries []Node
	for _, n := range s.nodes {
		if !n.primary {
			secondaries = append(secondaries, n.node)
		}
	}

	return secondaries, nil
}

func (s *LocalMemoryElectionStrategy) updateMetrics() {
	s.m.RLock()
	defer s.m.RUnlock()

	for _, node := range s.nodes {
		val := float64(0)

		if node.primary {
			val = float64(1)
		}

		metrics.PrimaryGauge.WithLabelValues(s.shardName, node.node.GetStorage()).Set(val)
	}
}
