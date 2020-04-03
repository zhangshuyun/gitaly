package nodes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
)

func TestPrimaryAndSecondaries(t *testing.T) {
	socket := testhelper.GetTemporaryGitalySocketFileName()
	svr, _ := testhelper.NewServerWithHealth(t, socket)
	defer svr.Stop()

	cc, err := grpc.Dial(
		"unix://"+socket,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"
	mockHistogramVec0, mockHistogramVec1 := promtest.NewMockHistogramVec(), promtest.NewMockHistogramVec()

	cs := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), mockHistogramVec0)
	secondary := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), mockHistogramVec1)
	ns := []*nodeStatus{cs, secondary}
	logger := testhelper.NewTestLogger(t).WithField("test", t.Name())
	strategy := newLocalElector(storageName, true, logger, ns)

	strategy.bootstrap(time.Second)

	primary, err := strategy.GetPrimary()

	require.NoError(t, err)
	require.Equal(t, primary, cs)

	secondaries, err := strategy.GetSecondaries()

	require.NoError(t, err)
	require.Equal(t, 1, len(secondaries))
	require.Equal(t, secondary, secondaries[0])
}
