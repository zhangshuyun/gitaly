package nodes

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/middleware"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
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
	cs := newConnectionStatus(models.Node{Storage: storageName}, cc, testhelper.DiscardTestEntry(t), middleware.NewErrors(0, 0, 0), mockHistogramVec)

	var expectedLabels [][]string
	for i := 0; i < healthcheckThreshold; i++ {
		ctx := context.Background()
		status, err := cs.check(ctx)

		require.NoError(t, err)
		require.True(t, status)
		expectedLabels = append(expectedLabels, []string{storageName})
	}

	require.Equal(t, expectedLabels, mockHistogramVec.LabelsCalled())
	require.Len(t, mockHistogramVec.Observer().Observed(), healthcheckThreshold)

	healthSvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	ctx := context.Background()
	status, err := cs.check(ctx)
	require.NoError(t, err)
	require.False(t, status)
}

func TestManagerFailoverDisabledElectionStrategySQL(t *testing.T) {
	const virtualStorageName = "virtual-storage-0"
	const primaryStorage = "praefect-internal-0"
	socket0, socket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	virtualStorage := &config.VirtualStorage{
		Name: virtualStorageName,
		Nodes: []*models.Node{
			{
				Storage:        primaryStorage,
				Address:        "unix://" + socket0,
				DefaultPrimary: true,
			},
			{
				Storage: "praefect-internal-1",
				Address: "unix://" + socket1,
			},
		},
	}

	srv0, healthSrv := testhelper.NewServerWithHealth(t, socket0)
	defer srv0.Stop()

	srv1, _ := testhelper.NewServerWithHealth(t, socket1)
	defer srv1.Stop()

	conf := config.Config{
		Failover:        config.Failover{Enabled: false, ElectionStrategy: "sql"},
		VirtualStorages: []*config.VirtualStorage{virtualStorage},
	}
	nm, err := NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, middleware.NewErrors(0, 0, 0), promtest.NewMockHistogramVec())
	require.NoError(t, err)

	nm.Start(time.Millisecond, time.Millisecond)

	shard, err := nm.GetShard(virtualStorageName)
	require.NoError(t, err)
	require.Equal(t, primaryStorage, shard.Primary.GetStorage())

	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.checkShards()

	shard, err = nm.GetShard(virtualStorageName)
	require.NoError(t, err)
	require.Equal(t, primaryStorage, shard.Primary.GetStorage())
}

func TestPrimaryIsSecond(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	_, healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	_, healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	virtualStorages := []*config.VirtualStorage{
		{
			Name: "virtual-storage-0",
			Nodes: []*models.Node{
				{
					Storage:        "praefect-internal-0",
					Address:        "unix://" + gitalySocket0,
					DefaultPrimary: false,
				},
				{
					Storage:        "praefect-internal-1",
					Address:        "unix://" + gitalySocket1,
					DefaultPrimary: true,
				},
			},
		},
	}

	conf := config.Config{
		VirtualStorages: virtualStorages,
		Failover:        config.Failover{Enabled: false},
	}

	mockHistogram := promtest.NewMockHistogramVec()
	nm, err := NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, middleware.NewErrors(0, 0, 0), mockHistogram)
	require.NoError(t, err)

	shard, err := nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	require.False(t, shard.IsReadOnly, "new shard should not be read-only")

	require.Equal(t, virtualStorages[0].Nodes[1].Storage, shard.Primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, shard.Primary.GetAddress())

	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, shard.Secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, shard.Secondaries[0].GetAddress())
}

func TestBlockingDial(t *testing.T) {
	storageName := "default"
	praefectSocket := testhelper.GetTemporaryGitalySocketFileName()
	socketName := "unix://" + praefectSocket

	gitalySocket := testhelper.GetTemporaryGitalySocketFileName()

	lis, err := net.Listen("unix", gitalySocket)
	require.NoError(t, err)

	conf := config.Config{
		SocketPath: socketName,
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: storageName,
				Nodes: []*models.Node{
					{
						Storage:        "internal-storage",
						Address:        "unix://" + gitalySocket,
						DefaultPrimary: true,
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	// simulate gitaly node starting up later
	go func() {
		time.Sleep(checkTimeout + 10*time.Millisecond)

		_, healthSrv0 := testhelper.NewHealthServerWithListener(t, lis)
		healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	}()

	mgr, err := NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, middleware.NewErrors(0, 0, 0), promtest.NewMockHistogramVec())
	require.NoError(t, err)

	mgr.Start(1*time.Millisecond, 1*time.Millisecond)

	shard, err := mgr.GetShard(storageName)
	require.NoError(t, err)
	require.Equal(t, "internal-storage", shard.Primary.GetStorage())
	require.Empty(t, shard.Secondaries)
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
		Failover:        config.Failover{Enabled: true, ReadOnlyAfterFailover: true},
	}
	confWithoutFailover := config.Config{
		VirtualStorages: virtualStorages,
		Failover:        config.Failover{Enabled: false},
	}

	mockHistogram := promtest.NewMockHistogramVec()
	nm, err := NewManager(testhelper.DiscardTestEntry(t), confWithFailover, nil, nil, middleware.NewErrors(0, 0, 0), mockHistogram)
	require.NoError(t, err)

	nmWithoutFailover, err := NewManager(testhelper.DiscardTestEntry(t), confWithoutFailover, nil, nil, middleware.NewErrors(0, 0, 0), mockHistogram)
	require.NoError(t, err)

	nm.Start(1*time.Millisecond, 5*time.Second)
	nmWithoutFailover.Start(1*time.Millisecond, 5*time.Second)

	_, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)

	shardWithoutFailover, err := nmWithoutFailover.GetShard("virtual-storage-0")
	require.NoError(t, err)

	shard, err := nm.GetShard("virtual-storage-0")
	require.NoError(t, err)

	// shard without failover and shard with failover should be the same
	require.Equal(t, shardWithoutFailover.Primary.GetStorage(), shard.Primary.GetStorage())
	require.Equal(t, shardWithoutFailover.Primary.GetAddress(), shard.Primary.GetAddress())
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, shardWithoutFailover.Secondaries[0].GetStorage(), shard.Secondaries[0].GetStorage())
	require.Equal(t, shardWithoutFailover.Secondaries[0].GetAddress(), shard.Secondaries[0].GetAddress())
	require.False(t, shard.IsReadOnly)
	require.False(t, shardWithoutFailover.IsReadOnly)

	require.Equal(t, virtualStorages[0].Nodes[0].Storage, shard.Primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, shard.Primary.GetAddress())
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, shard.Secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, shard.Secondaries[0].GetAddress())

	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.checkShards()

	labelsCalled := mockHistogram.LabelsCalled()
	for _, node := range virtualStorages[0].Nodes {
		require.Contains(t, labelsCalled, []string{node.Storage})
	}

	// since the primary is unhealthy, we expect checkShards to demote primary to secondary, and promote the healthy
	// secondary to primary

	shardWithoutFailover, err = nmWithoutFailover.GetShard("virtual-storage-0")
	require.NoError(t, err)

	shard, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)

	// shard without failover and shard with failover should not be the same
	require.NotEqual(t, shardWithoutFailover.Primary.GetStorage(), shard.Primary.GetStorage())
	require.NotEqual(t, shardWithoutFailover.Primary.GetAddress(), shard.Primary.GetAddress())
	require.NotEqual(t, shardWithoutFailover.Secondaries[0].GetStorage(), shard.Secondaries[0].GetStorage())
	require.NotEqual(t, shardWithoutFailover.Secondaries[0].GetAddress(), shard.Secondaries[0].GetAddress())

	// shard without failover should still match the config
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, shardWithoutFailover.Primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, shardWithoutFailover.Primary.GetAddress())
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, shardWithoutFailover.Secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, shardWithoutFailover.Secondaries[0].GetAddress())
	require.False(t, shardWithoutFailover.IsReadOnly,
		"shard should not be read-only after primary failure with failover disabled")

	// shard with failover should have promoted a secondary to primary and demoted the primary to a secondary
	require.Equal(t, virtualStorages[0].Nodes[1].Storage, shard.Primary.GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[1].Address, shard.Primary.GetAddress())
	require.Len(t, shard.Secondaries, 1)
	require.Equal(t, virtualStorages[0].Nodes[0].Storage, shard.Secondaries[0].GetStorage())
	require.Equal(t, virtualStorages[0].Nodes[0].Address, shard.Secondaries[0].GetAddress())
	require.True(t, shard.IsReadOnly, "shard should be read-only after a failover")

	require.NoError(t, nm.EnableWrites(context.Background(), "virtual-storage-0"))
	shard, err = nm.GetShard("virtual-storage-0")
	require.NoError(t, err)
	require.False(t, shard.IsReadOnly, "shard should be write enabled")

	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_UNKNOWN)
	nm.checkShards()

	_, err = nm.GetShard("virtual-storage-0")
	require.Error(t, err, "should return error since no nodes are healthy")
	require.Equal(t, ErrPrimaryNotHealthy, nm.EnableWrites(context.Background(), "virtual-storage-0"),
		"should not be able to enable writes with unhealthy master")
}

func TestMgr_GetSyncedNode(t *testing.T) {
	var sockets [4]string
	var srvs [4]*grpc.Server
	var healthSrvs [4]*health.Server
	for i := 0; i < 4; i++ {
		sockets[i] = testhelper.GetTemporaryGitalySocketFileName()
		srvs[i], healthSrvs[i] = testhelper.NewServerWithHealth(t, sockets[i])
		defer srvs[i].Stop()
	}

	vs0Primary := "unix://" + sockets[0]
	vs1Secondary := "unix://" + sockets[3]

	virtualStorages := []*config.VirtualStorage{
		{
			Name: "virtual-storage-0",
			Nodes: []*models.Node{
				{
					Storage:        "gitaly-0",
					Address:        vs0Primary,
					DefaultPrimary: true,
				},
				{
					Storage: "gitaly-1",
					Address: "unix://" + sockets[1],
				},
			},
		},
		{
			// second virtual storage needed to check there is no intersections between two even with same storage names
			Name: "virtual-storage-1",
			Nodes: []*models.Node{
				{
					// same storage name as in other virtual storage is used intentionally
					Storage:        "gitaly-1",
					Address:        "unix://" + sockets[2],
					DefaultPrimary: true,
				},
				{
					Storage: "gitaly-2",
					Address: vs1Secondary,
				},
			},
		},
	}

	conf := config.Config{
		VirtualStorages: virtualStorages,
		Failover:        config.Failover{Enabled: true},
	}

	mockHistogram := promtest.NewMockHistogramVec()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, featureflag.DistributedReads)

	ackEvent := func(queue datastore.ReplicationEventQueue, job datastore.ReplicationJob, state datastore.JobState) datastore.ReplicationEvent {
		event := datastore.ReplicationEvent{Job: job}

		eevent, err := queue.Enqueue(ctx, event)
		require.NoError(t, err)

		devents, err := queue.Dequeue(ctx, eevent.Job.VirtualStorage, eevent.Job.TargetNodeStorage, 2)
		require.NoError(t, err)
		require.Len(t, devents, 1)

		acks, err := queue.Acknowledge(ctx, state, []uint64{devents[0].ID})
		require.NoError(t, err)
		require.Equal(t, []uint64{devents[0].ID}, acks)
		return devents[0]
	}

	verify := func(scenario func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue)) func(*testing.T) {
		queue := datastore.NewMemoryReplicationEventQueue(conf)

		nm, err := NewManager(testhelper.DiscardTestEntry(t), conf, nil, queue, middleware.NewErrors(0, 0, 0), mockHistogram)
		require.NoError(t, err)

		nm.Start(time.Duration(0), time.Hour)

		return func(t *testing.T) { scenario(t, nm, queue) }
	}

	t.Run("unknown virtual storage", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		_, err := nm.GetSyncedNode(ctx, "virtual-storage-unknown", "")
		require.True(t, errors.Is(err, ErrVirtualStorageNotExist))
	}))

	t.Run("no replication events", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		node, err := nm.GetSyncedNode(ctx, "virtual-storage-0", "no/matter")
		require.NoError(t, err)
		require.Contains(t, []string{vs0Primary, "unix://" + sockets[1]}, node.GetAddress())
	}))

	t.Run("last replication event is in 'ready'", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		_, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
			Job: datastore.ReplicationJob{
				RelativePath:      "path/1",
				TargetNodeStorage: "gitaly-1",
				SourceNodeStorage: "gitaly-0",
				VirtualStorage:    "virtual-storage-0",
			},
		})
		require.NoError(t, err)

		node, err := nm.GetSyncedNode(ctx, "virtual-storage-0", "path/1")
		require.NoError(t, err)
		require.Equal(t, vs0Primary, node.GetAddress())
	}))

	t.Run("last replication event is in 'in_progress'", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		vs0Event, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
			Job: datastore.ReplicationJob{
				RelativePath:      "path/1",
				TargetNodeStorage: "gitaly-1",
				SourceNodeStorage: "gitaly-0",
				VirtualStorage:    "virtual-storage-0",
			},
		})
		require.NoError(t, err)

		vs0Events, err := queue.Dequeue(ctx, vs0Event.Job.VirtualStorage, vs0Event.Job.TargetNodeStorage, 100500)
		require.NoError(t, err)
		require.Len(t, vs0Events, 1)

		node, err := nm.GetSyncedNode(ctx, "virtual-storage-0", "path/1")
		require.NoError(t, err)
		require.Equal(t, vs0Primary, node.GetAddress())
	}))

	t.Run("last replication event is in 'failed'", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		vs0Event := ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "virtual-storage-0",
		}, datastore.JobStateFailed)

		node, err := nm.GetSyncedNode(ctx, vs0Event.Job.VirtualStorage, vs0Event.Job.RelativePath)
		require.NoError(t, err)
		require.Equal(t, vs0Primary, node.GetAddress())
	}))

	t.Run("multiple replication events for same virtual, last is in 'ready'", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		vsEvent0 := ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "virtual-storage-0",
		}, datastore.JobStateCompleted)

		vsEvent1, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
			Job: datastore.ReplicationJob{
				RelativePath:      vsEvent0.Job.RelativePath,
				TargetNodeStorage: vsEvent0.Job.TargetNodeStorage,
				SourceNodeStorage: vsEvent0.Job.SourceNodeStorage,
				VirtualStorage:    vsEvent0.Job.VirtualStorage,
			},
		})
		require.NoError(t, err)

		node, err := nm.GetSyncedNode(ctx, vsEvent1.Job.VirtualStorage, vsEvent1.Job.RelativePath)
		require.NoError(t, err)
		require.Equal(t, vs0Primary, node.GetAddress())
	}))

	t.Run("same repo path for different virtual storages", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		vs0Event := ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "virtual-storage-0",
		}, datastore.JobStateDead)

		ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "gitaly-1",
			VirtualStorage:    "virtual-storage-1",
		}, datastore.JobStateCompleted)

		node, err := nm.GetSyncedNode(ctx, vs0Event.Job.VirtualStorage, vs0Event.Job.RelativePath)
		require.NoError(t, err)
		require.Equal(t, vs0Primary, node.GetAddress())
	}))

	t.Run("secondary is up to date in multi-virtual setup with processed replication jobs", verify(func(t *testing.T, nm Manager, queue datastore.ReplicationEventQueue) {
		ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-1",
			SourceNodeStorage: "gitaly-0",
			VirtualStorage:    "virtual-storage-0",
		}, datastore.JobStateCompleted)

		ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/1",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "gitaly-1",
			VirtualStorage:    "virtual-storage-1",
		}, datastore.JobStateCompleted)

		vs1Event := ackEvent(queue, datastore.ReplicationJob{
			RelativePath:      "path/2",
			TargetNodeStorage: "gitaly-2",
			SourceNodeStorage: "gitaly-1",
			VirtualStorage:    "virtual-storage-1",
		}, datastore.JobStateCompleted)

		node, err := nm.GetSyncedNode(ctx, vs1Event.Job.VirtualStorage, vs1Event.Job.RelativePath)
		require.NoError(t, err)
		require.Equal(t, vs1Secondary, node.GetAddress())
	}))
}
