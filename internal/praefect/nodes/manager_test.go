package nodes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestNodeStatus(t *testing.T) {
	socket := testhelper.GetTemporaryGitalySocketFileName()
	svr, healthSvr := testhelper.NewServerWithHealth(t, socket)
	defer svr.Stop()

	cc, err := grpc.Dial(
		"unix://"+socket,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	mockHistogramVec := promtest.NewMockHistogramVec()

	storageName := "default"
	cs := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), mockHistogramVec)

	var expectedLabels [][]string
	for i := 0; i < healthcheckThreshold; i++ {
		require.True(t, cs.check())
		expectedLabels = append(expectedLabels, []string{storageName})
	}

	require.Equal(t, expectedLabels, mockHistogramVec.LabelsCalled())
	require.Len(t, mockHistogramVec.Observer().Observed(), healthcheckThreshold)

	healthSvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	require.False(t, cs.check())
}

func TestNodeManager(t *testing.T) {
	internalSocket0, internalSocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	srv0, healthSrv0 := testhelper.NewServerWithHealth(t, internalSocket0)
	defer srv0.Stop()

	srv1, healthSrv1 := testhelper.NewServerWithHealth(t, internalSocket1)
	defer srv1.Stop()

	virtualStorages := []*config.VirtualStorage{
		{
			Name: "virtual-storage-0",
			Nodes: []*models.Node{
				{
					Storage:        "praefect-internal-0",
					Address:        "unix://" + internalSocket0,
					DefaultPrimary: true,
				},
				{
					Storage: "praefect-internal-1",
					Address: "unix://" + internalSocket1,
				},
			},
		},
	}

	confWithFailover := config.Config{
		VirtualStorages: virtualStorages,
		FailoverEnabled: true,
	}
	confWithoutFailover := config.Config{
		VirtualStorages: virtualStorages,
		FailoverEnabled: false,
	}

	mockHistogram := promtest.NewMockHistogramVec()
	nm, err := NewManager(testhelper.DiscardTestEntry(t), confWithFailover, mockHistogram)
	require.NoError(t, err)

	nmWithoutFailover, err := NewManager(testhelper.DiscardTestEntry(t), confWithoutFailover, mockHistogram)
	require.NoError(t, err)

	nm.Start(1*time.Millisecond, 5*time.Second)
	nmWithoutFailover.Start(1*time.Millisecond, 5*time.Second)

	_, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)

	shardWithoutFailover, err := nmWithoutFailover.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primaryWithoutFailover, err := shardWithoutFailover.GetPrimary()
	require.NoError(t, err)
	secondariesWithoutFailover, err := shardWithoutFailover.GetSecondaries()
	require.NoError(t, err)

	shard, err := nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primary, err := shard.GetPrimary()
	require.NoError(t, err)
	secondaries, err := shard.GetSecondaries()
	require.NoError(t, err)

	// shard without failover and shard with failover should be the same
	require.Equal(t, primaryWithoutFailover.GetStorage(), primary.GetStorage())
	require.Equal(t, primaryWithoutFailover.GetAddress(), primary.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, secondariesWithoutFailover[0].GetStorage(), secondaries[0].GetStorage())
	require.Equal(t, secondariesWithoutFailover[0].GetAddress(), secondaries[0].GetAddress())

	require.Equal(t, virtualStorages[0].Nodes[0].Storage, primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, primary.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, secondaries[0].GetAddress())

	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.CheckShards()

	labelsCalled := mockHistogram.LabelsCalled()
	for _, node := range virtualStorages[0].Nodes {
		require.Contains(t, labelsCalled, []string{node.Storage})
	}

	// since the primary is unhealthy, we expect checkShards to demote primary to secondary, and promote the healthy
	// secondary to primary

	shardWithoutFailover, err = nmWithoutFailover.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primaryWithoutFailover, err = shardWithoutFailover.GetPrimary()
	require.NoError(t, err)
	secondariesWithoutFailover, err = shardWithoutFailover.GetSecondaries()
	require.NoError(t, err)

	shard, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primary, err = shard.GetPrimary()
	require.NoError(t, err)
	secondaries, err = shard.GetSecondaries()
	require.NoError(t, err)

	// shard without failover and shard with failover should not be the same
	require.NotEqual(t, primaryWithoutFailover.GetStorage(), primary.GetStorage())
	require.NotEqual(t, primaryWithoutFailover.GetAddress(), primary.GetAddress())
	require.NotEqual(t, secondariesWithoutFailover[0].GetStorage(), secondaries[0].GetStorage())
	require.NotEqual(t, secondariesWithoutFailover[0].GetAddress(), secondaries[0].GetAddress())

	// shard without failover should still match the config
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, primaryWithoutFailover.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, primaryWithoutFailover.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, secondariesWithoutFailover[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, secondariesWithoutFailover[0].GetAddress())

	// shard with failover should have promoted a secondary to primary and demoted the primary to a secondary
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, primary.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, secondaries[0].GetAddress())

	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.CheckShards()

	_, err = nm.GetShard("virtual-storage-0")
	require.Error(t, err, "should return error since no nodes are healthy")
}
