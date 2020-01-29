package nodes

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestNodeStatus(t *testing.T) {
	cc, healthSvr, cleanup := newHealthServer(t, testhelper.GetTemporaryGitalySocketFileName())
	defer cleanup()

	cs := newConnectionStatus(models.Node{}, cc)

	require.False(t, cs.isHealthy())

	for i := 0; i < healthcheckThreshold; i++ {
		require.NoError(t, cs.check())
	}
	require.True(t, cs.isHealthy())

	healthSvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	require.NoError(t, cs.check())
	require.False(t, cs.isHealthy())
}

func TestNodeManager(t *testing.T) {
	internalSocket0 := testhelper.GetTemporaryGitalySocketFileName()
	internalSocket1 := testhelper.GetTemporaryGitalySocketFileName()

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

	_, srv0, cancel0 := newHealthServer(t, internalSocket0)
	defer cancel0()

	_, _, cancel1 := newHealthServer(t, internalSocket1)
	defer cancel1()

	nm, err := NewManager(log.Default(), virtualStorages)
	require.NoError(t, err)

	_, err = nm.GetShard("virtual-storage-0")
	require.Error(t, ErrPrimaryNotHealthy, err)

	nm.Start(1*time.Millisecond, 5*time.Second)

	shard, err := nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primary, err := shard.GetPrimary()
	require.NoError(t, err)
	secondaries, err := shard.GetSecondaries()
	require.NoError(t, err)

	require.Equal(t, virtualStorages[0].Nodes[0].Storage, primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, primary.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, secondaries[0].GetAddress())

	srv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.checkShards()

	// since the primary is unhealthy, we expect checkShards to demote primary to secondary, and promote the healthy
	// secondary to primary

	shard, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	primary, err = shard.GetPrimary()
	require.NoError(t, err)
	secondaries, err = shard.GetSecondaries()
	require.NoError(t, err)

	require.Equal(t, virtualStorages[0].Nodes[1].Storage, primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, primary.GetAddress())
	require.Len(t, secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, secondaries[0].GetAddress())

	cancel1()
	nm.checkShards()

	_, err = nm.GetShard("virtual-storage-0")
	require.Error(t, err, "should return error since no nodes are healthy")
}

func newHealthServer(t testing.TB, socketName string) (*grpc.ClientConn, *health.Server, func()) {
	srv := testhelper.NewTestGrpcServer(t, nil, nil)
	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthSrvr)
	healthSrvr.SetServingStatus("TestService", grpc_health_v1.HealthCheckResponse_SERVING)

	lis, err := net.Listen("unix", socketName)
	require.NoError(t, err)

	go srv.Serve(lis)

	cleanup := func() {
		srv.Stop()
	}

	cc, err := grpc.Dial(
		"unix://"+socketName,
		grpc.WithInsecure(),
	)

	require.NoError(t, err)

	return cc, healthSrvr, cleanup
}
