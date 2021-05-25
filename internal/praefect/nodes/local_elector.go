package nodes

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/metrics"
)

// localElector relies on an in-memory datastore to track the primary
// and secondaries. A single election strategy pertains to a single
// shard. It does NOT support multiple Praefect nodes or have any
// persistence. This is used mostly for testing and development.
type localElector struct {
	m           sync.RWMutex
	shardName   string
	nodes       []Node
	primaryNode Node
	log         logrus.FieldLogger
}

func newLocalElector(name string, log logrus.FieldLogger, ns []*nodeStatus) *localElector {
	nodes := make([]Node, len(ns))
	for i, n := range ns {
		nodes[i] = n
	}
	return &localElector{
		shardName:   name,
		log:         log.WithField("virtual_storage", name),
		nodes:       nodes[:],
		primaryNode: nodes[0],
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

// checkNodes issues a gRPC health check for each node managed by the
// shard.
func (s *localElector) checkNodes(ctx context.Context) error {
	defer s.updateMetrics()

	var wg sync.WaitGroup
	for _, n := range s.nodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			_, _ = n.CheckHealth(ctx)
		}(n)
	}
	wg.Wait()

	s.m.Lock()
	defer s.m.Unlock()

	if s.primaryNode.IsHealthy() {
		return nil
	}

	var newPrimary Node

	for _, node := range s.nodes {
		if node != s.primaryNode && node.IsHealthy() {
			newPrimary = node
			break
		}
	}

	if newPrimary == nil {
		return ErrPrimaryNotHealthy
	}

	s.primaryNode = newPrimary

	return nil
}

// GetShard gets the current status of the shard. If primary is not elected
// or it is unhealthy and failover is enabled, ErrPrimaryNotHealthy is
// returned.
func (s *localElector) GetShard(ctx context.Context) (Shard, error) {
	s.m.RLock()
	primary := s.primaryNode
	s.m.RUnlock()

	if primary == nil {
		return Shard{}, ErrPrimaryNotHealthy
	}

	if !primary.IsHealthy() {
		return Shard{}, ErrPrimaryNotHealthy
	}

	var secondaries []Node
	for _, n := range s.nodes {
		if n != primary {
			secondaries = append(secondaries, n)
		}
	}

	return Shard{
		Primary:     primary,
		Secondaries: secondaries,
	}, nil
}

func (s *localElector) updateMetrics() {
	s.m.RLock()
	primary := s.primaryNode
	s.m.RUnlock()

	for _, n := range s.nodes {
		var val float64

		if n == primary {
			val = 1
		}

		metrics.PrimaryGauge.WithLabelValues(s.shardName, n.GetStorage()).Set(val)
	}
}
