package nodes

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
)

func setupElector(t *testing.T) (*localElector, []*nodeStatus, *grpc.ClientConn, *grpc.Server) {
	socket := testhelper.GetTemporaryGitalySocketFileName()
	svr, _ := testhelper.NewServerWithHealth(t, socket)

	cc, err := grpc.Dial(
		"unix://"+socket,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0, mockHistogramVec1 := promtest.NewMockHistogramVec(), promtest.NewMockHistogramVec()

	errTracker := middleware.NewErrors(0, 0, 0)
	cs := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), errTracker, mockHistogramVec0)
	secondary := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), errTracker, mockHistogramVec1)
	ns := []*nodeStatus{cs, secondary}
	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	strategy := newLocalElector(storageName, true, true, logger, ns)

	strategy.bootstrap(time.Second)

	return strategy, ns, cc, svr
}

func TestGetShard(t *testing.T) {
	strategy, ns, _, svr := setupElector(t)
	defer svr.Stop()

	shard, err := strategy.GetShard()
	require.NoError(t, err)
	require.False(t, shard.IsReadOnly, "new shard should not be read-only")
	require.Equal(t, ns[0], shard.Primary)
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, ns[1], shard.Secondaries[0])
}

func TestConcurrentCheckWithPrimary(t *testing.T) {
	strategy, ns, _, svr := setupElector(t)
	defer svr.Stop()

	iterations := 10
	var wg sync.WaitGroup
	start := make(chan bool)
	wg.Add(2)

	go func() {
		defer wg.Done()

		ctx, cancel := testhelper.Context()
		defer cancel()

		<-start

		for i := 0; i < iterations; i++ {
			strategy.checkNodes(ctx)
		}
	}()

	go func() {
		defer wg.Done()
		start <- true

		for i := 0; i < iterations; i++ {
			shard, err := strategy.GetShard()
			require.NoError(t, err)
			require.Equal(t, ns[0], shard.Primary)
			require.Equal(t, 1, len(shard.Secondaries))
			require.Equal(t, ns[1], shard.Secondaries[0])
		}
	}()

	wg.Wait()
}
