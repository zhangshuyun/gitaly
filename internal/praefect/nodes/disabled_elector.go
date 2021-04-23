package nodes

import (
	"context"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
)

// newDisabledElector returns a stub that always returns the same shard where the
// primary is the first node from the passed in list.
func newDisabledElector(virtualStorage string, ns []*nodeStatus) *disabledElector {
	secondaries := make([]Node, len(ns)-1)
	for i, node := range ns[1:] {
		secondaries[i] = node
	}
	return &disabledElector{
		virtualStorage: virtualStorage,
		shard:          Shard{Primary: ns[0], Secondaries: secondaries},
	}
}

type disabledElector struct {
	shard          Shard
	virtualStorage string
}

func (de *disabledElector) start(bootstrap, _ time.Duration) {
	timer := time.NewTimer(bootstrap)
	defer timer.Stop()

	for i := 0; i < healthcheckThreshold; i++ {
		<-timer.C
		ctx := context.TODO()
		_ = de.checkNodes(ctx)
		timer.Reset(bootstrap)
	}

	de.updateMetrics()
}

func (de *disabledElector) updateMetrics() {
	metrics.PrimaryGauge.WithLabelValues(de.virtualStorage, de.shard.Primary.GetStorage()).Set(1)
	for _, n := range de.shard.Secondaries {
		metrics.PrimaryGauge.WithLabelValues(de.virtualStorage, n.GetStorage()).Set(0)
	}
}

func (de *disabledElector) checkNodes(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, n := range append(de.shard.Secondaries, de.shard.Primary) {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			_, _ = n.CheckHealth(ctx)
		}(n)
	}
	wg.Wait()
	return nil
}

func (de *disabledElector) GetShard(ctx context.Context) (Shard, error) {
	if !de.shard.Primary.IsHealthy() {
		return Shard{}, ErrPrimaryNotHealthy
	}

	return de.shard, nil
}
