package nodes

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
)

func TestPrimaryAndSecondaries(t *testing.T) {
	socket := testhelper.GetTemporaryGitalySocketFileName()

	cc, err := grpc.Dial(
		"unix://"+socket,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	storageName := "default"
	cs := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), nil)
	strategy := newLocalMemoryElectionStrategy(storageName, true)

	strategy.AddNode(cs, true)

	primary, err := strategy.GetPrimary()

	require.NoError(t, err)
	require.Equal(t, primary, cs)

	secondaries, err := strategy.GetSecondaries()

	require.NoError(t, err)
	require.Equal(t, 0, len(secondaries))

	secondary := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), nil)
	strategy.AddNode(secondary, false)

	secondaries, err = strategy.GetSecondaries()

	require.NoError(t, err)
	require.Equal(t, 1, len(secondaries))
	require.Equal(t, secondary, secondaries[0])
}
