package nodes

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
)

func setupElector(t *testing.T) (*localElector, []*nodeStatus, *grpc.ClientConn) {
	socket := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, socket)

	cc, err := grpc.Dial(
		"unix://"+socket,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0, mockHistogramVec1 := promtest.NewMockHistogramVec(), promtest.NewMockHistogramVec()

	cs := newConnectionStatus(config.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), mockHistogramVec0, nil)
	secondary := newConnectionStatus(config.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), mockHistogramVec1, nil)
	ns := []*nodeStatus{cs, secondary}
	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	strategy := newLocalElector(storageName, logger, ns)

	strategy.bootstrap(time.Second)

	return strategy, ns, cc
}

func TestGetShard(t *testing.T) {
	strategy, ns, _ := setupElector(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	shard, err := strategy.GetShard(ctx)
	require.NoError(t, err)
	require.Equal(t, ns[0], shard.Primary)
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, ns[1], shard.Secondaries[0])
}

func TestConcurrentCheckWithPrimary(t *testing.T) {
	strategy, ns, _ := setupElector(t)

	iterations := 10
	var wg sync.WaitGroup
	start := make(chan bool)
	wg.Add(2)

	ctx, cancel := testhelper.Context()
	defer cancel()

	go func() {
		defer wg.Done()

		<-start

		for i := 0; i < iterations; i++ {
			strategy.checkNodes(ctx)
		}
	}()

	go func() {
		defer wg.Done()
		start <- true

		for i := 0; i < iterations; i++ {
			shard, err := strategy.GetShard(ctx)
			require.NoError(t, err)
			require.Equal(t, ns[0], shard.Primary)
			require.Equal(t, 1, len(shard.Secondaries))
			require.Equal(t, ns[1], shard.Secondaries[0])
		}
	}()

	wg.Wait()
}
