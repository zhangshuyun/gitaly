package praefect

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	gconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestReplMgr_ProcessBacklog(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	primaryCfg, testRepoProto, testRepoPath := testcfg.BuildWithRepo(t, testcfg.WithStorages("primary"))
	testRepo := localrepo.NewTestRepo(t, primaryCfg, testRepoProto)
	primaryCfg.SocketPath = testserver.RunGitalyServer(t, primaryCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	testcfg.BuildGitalySSH(t, primaryCfg)
	testcfg.BuildGitalyHooks(t, primaryCfg)

	backupCfg, _, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages("backup"))
	backupCfg.SocketPath = testserver.RunGitalyServer(t, backupCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	testcfg.BuildGitalySSH(t, backupCfg)
	testcfg.BuildGitalyHooks(t, backupCfg)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{{
			Name: "virtual",
			Nodes: []*config.Node{
				{
					Storage: primaryCfg.Storages[0].Name,
					Address: primaryCfg.SocketPath,
				},
				{
					Storage: backupCfg.Storages[0].Name,
					Address: backupCfg.SocketPath,
				},
			},
		}},
	}

	txManager := transaction.NewManager(primaryCfg, backchannel.NewRegistry())

	// create object pool on the source
	objectPoolPath := gittest.NewObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(
		gconfig.NewLocator(primaryCfg),
		gittest.NewCommandFactory(t, primaryCfg),
		nil,
		txManager,
		housekeeping.NewManager(txManager),
		testRepoProto.GetStorageName(),
		objectPoolPath,
	)
	require.NoError(t, err)

	poolCtx := testhelper.Context(t)
	require.NoError(t, pool.Create(poolCtx, testRepo))
	require.NoError(t, pool.Link(poolCtx, testRepo))

	// replicate object pool repository to target node
	poolRepository := pool.ToProto().GetRepository()
	targetObjectPoolRepo := proto.Clone(poolRepository).(*gitalypb.Repository)
	targetObjectPoolRepo.StorageName = backupCfg.Storages[0].Name
	ctx, cancel := context.WithCancel(ctx)

	injectedCtx := metadata.NewOutgoingContext(ctx, testcfg.GitalyServersMetadataFromCfg(t, primaryCfg))

	repoClient := newRepositoryClient(t, backupCfg.SocketPath, backupCfg.Auth.Token)
	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetObjectPoolRepo,
		Source:     poolRepository,
	})
	require.NoError(t, err)

	entry := testhelper.NewDiscardingLogEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)
	nodeMgr.Start(1*time.Millisecond, 5*time.Millisecond)
	defer nodeMgr.Stop()

	shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
	require.NoError(t, err)
	require.Len(t, shard.Secondaries, 1)

	const repositoryID = 1
	var events []datastore.ReplicationEvent
	for _, secondary := range shard.Secondaries {
		events = append(events, datastore.ReplicationEvent{
			Job: datastore.ReplicationJob{
				RepositoryID:      repositoryID,
				VirtualStorage:    conf.VirtualStorages[0].Name,
				Change:            datastore.UpdateRepo,
				TargetNodeStorage: secondary.GetStorage(),
				SourceNodeStorage: shard.Primary.GetStorage(),
				RelativePath:      testRepo.GetRelativePath(),
			},
			State:   datastore.JobStateReady,
			Attempt: 3,
			Meta:    datastore.Params{metadatahandler.CorrelationIDKey: "correlation-id"},
		})
	}
	require.Len(t, events, 1)

	commitID := gittest.WriteCommit(t, primaryCfg, testRepoPath, gittest.WithBranch("master"))

	var mockReplicationLatencyHistogramVec promtest.MockHistogramVec
	var mockReplicationDelayHistogramVec promtest.MockHistogramVec

	logger := testhelper.NewDiscardingLogger(t)
	loggerHook := test.NewLocal(logger)

	queue := datastore.NewReplicationEventQueueInterceptor(datastore.NewPostgresReplicationEventQueue(testdb.New(t)))
	queue.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		cancel() // when it is called we know that replication is finished
		return queue.Acknowledge(ctx, state, ids)
	})

	loggerEntry := logger.WithField("test", t.Name())
	_, err = queue.Enqueue(ctx, events[0])
	require.NoError(t, err)

	db := testdb.New(t)
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
	require.NoError(t, rs.CreateRepository(ctx, repositoryID, conf.VirtualStorages[0].Name, testRepo.GetRelativePath(), testRepo.GetRelativePath(), shard.Primary.GetStorage(), nil, nil, true, false))

	replMgr := NewReplMgr(
		loggerEntry,
		conf.StorageNames(),
		queue,
		rs,
		nodeMgr,
		NodeSetFromNodeManager(nodeMgr),
		WithLatencyMetric(&mockReplicationLatencyHistogramVec),
		WithDelayMetric(&mockReplicationDelayHistogramVec),
		WithParallelStorageProcessingWorkers(100),
	)

	replMgr.ProcessBacklog(ctx, noopBackoffFactory{})

	logEntries := loggerHook.AllEntries()
	require.True(t, len(logEntries) > 4, "expected at least 5 log entries to be present")
	require.Equal(t,
		[]interface{}{`parallel processing workers decreased from 100 configured with config to 1 according to minumal amount of storages in the virtual storage "virtual"`},
		[]interface{}{logEntries[0].Message},
	)

	require.Equal(t,
		[]interface{}{"processing started", "virtual"},
		[]interface{}{logEntries[1].Message, logEntries[1].Data["virtual_storage"]},
	)

	require.Equal(t,
		[]interface{}{"replication job processing started", "virtual", "correlation-id"},
		[]interface{}{logEntries[2].Message, logEntries[2].Data["virtual_storage"], logEntries[2].Data[logWithCorrID]},
	)

	dequeuedEvent := logEntries[2].Data["event"].(datastore.ReplicationEvent)
	require.Equal(t, datastore.JobStateInProgress, dequeuedEvent.State)
	require.Equal(t, []string{"backup", "primary"}, []string{dequeuedEvent.Job.TargetNodeStorage, dequeuedEvent.Job.SourceNodeStorage})

	require.Equal(t,
		[]interface{}{"replication job processing finished", "virtual", datastore.JobStateCompleted, "correlation-id"},
		[]interface{}{logEntries[3].Message, logEntries[3].Data["virtual_storage"], logEntries[3].Data["new_state"], logEntries[3].Data[logWithCorrID]},
	)

	replicatedPath := filepath.Join(backupCfg.Storages[0].Path, testRepo.GetRelativePath())

	gittest.Exec(t, backupCfg, "-C", replicatedPath, "cat-file", "-e", commitID.String())
	gittest.Exec(t, backupCfg, "-C", replicatedPath, "gc")
	require.Less(t, gittest.GetGitPackfileDirSize(t, replicatedPath), int64(100), "expect a small pack directory")

	require.Equal(t, mockReplicationLatencyHistogramVec.LabelsCalled(), [][]string{{"update"}})
	require.Equal(t, mockReplicationDelayHistogramVec.LabelsCalled(), [][]string{{"update"}})
	require.NoError(t, testutil.CollectAndCompare(replMgr, strings.NewReader(`
# HELP gitaly_praefect_replication_jobs Number of replication jobs in flight.
# TYPE gitaly_praefect_replication_jobs gauge
gitaly_praefect_replication_jobs{change_type="update",gitaly_storage="backup",virtual_storage="virtual"} 0
`)))
}

func TestReplicatorDowngradeAttempt(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = correlation.ContextWithCorrelation(ctx, "correlation-id")

	for _, tc := range []struct {
		desc                string
		attemptedGeneration int
		expectedMessage     string
	}{
		{
			desc:                "same generation attempted",
			attemptedGeneration: 1,
			expectedMessage:     "target repository already on the same generation, skipping replication job",
		},
		{
			desc:                "lower generation attempted",
			attemptedGeneration: 0,
			expectedMessage:     "repository downgrade prevented",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			returnedErr := datastore.DowngradeAttemptedError{
				Storage:             "gitaly-2",
				CurrentGeneration:   1,
				AttemptedGeneration: tc.attemptedGeneration,
			}

			rs := datastore.MockRepositoryStore{
				GetReplicatedGenerationFunc: func(ctx context.Context, repositoryID int64, source, target string) (int, error) {
					return 0, returnedErr
				},
			}

			logger := testhelper.NewDiscardingLogger(t)
			hook := test.NewLocal(logger)
			r := &defaultReplicator{rs: rs, log: logger}

			require.NoError(t, r.Replicate(ctx, datastore.ReplicationEvent{
				Job: datastore.ReplicationJob{
					ReplicaPath:       "relative-path-1",
					VirtualStorage:    "virtual-storage-1",
					RelativePath:      "relative-path-1",
					SourceNodeStorage: "gitaly-1",
					TargetNodeStorage: "gitaly-2",
				},
			}, nil, nil))

			require.Equal(t, logrus.InfoLevel, hook.LastEntry().Level)
			require.Equal(t, returnedErr, hook.LastEntry().Data["error"])
			require.Equal(t, "correlation-id", hook.LastEntry().Data[logWithCorrID])
			require.Equal(t, tc.expectedMessage, hook.LastEntry().Message)
		})
	}
}

func TestConfirmReplication(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, testRepoA, testRepoAPath := testcfg.BuildWithRepo(t)
	srvSocketPath := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	testRepoB, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)),
	}
	conn, err := grpc.Dial(srvSocketPath, connOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	equal, err := confirmChecksums(ctx, testhelper.NewDiscardingLogger(t), gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.True(t, equal)

	gittest.WriteCommit(t, cfg, testRepoAPath, gittest.WithBranch("master"))

	equal, err = confirmChecksums(ctx, testhelper.NewDiscardingLogger(t), gitalypb.NewRepositoryServiceClient(conn), gitalypb.NewRepositoryServiceClient(conn), testRepoA, testRepoB)
	require.NoError(t, err)
	require.False(t, equal)
}

func confirmChecksums(ctx context.Context, logger logrus.FieldLogger, primaryClient, replicaClient gitalypb.RepositoryServiceClient, primary, replica *gitalypb.Repository) (bool, error) {
	g, gCtx := errgroup.WithContext(ctx)

	var primaryChecksum, replicaChecksum string

	g.Go(getChecksumFunc(gCtx, primaryClient, primary, &primaryChecksum))
	g.Go(getChecksumFunc(gCtx, replicaClient, replica, &replicaChecksum))

	if err := g.Wait(); err != nil {
		return false, err
	}

	logger.WithFields(logrus.Fields{
		"primary_checksum": primaryChecksum,
		"replica_checksum": replicaChecksum,
	}).Info("checksum comparison completed")

	return primaryChecksum == replicaChecksum, nil
}

func getChecksumFunc(ctx context.Context, client gitalypb.RepositoryServiceClient, repo *gitalypb.Repository, checksum *string) func() error {
	return func() error {
		primaryChecksumRes, err := client.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
			Repository: repo,
		})
		if err != nil {
			return err
		}
		*checksum = primaryChecksumRes.GetChecksum()
		return nil
	}
}

func TestProcessBacklog_FailedJobs(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	primaryCfg, testRepo, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages("default"))
	primaryAddr := testserver.RunGitalyServer(t, primaryCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	backupCfg, _, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages("backup"))
	backupAddr := testserver.RunGitalyServer(t, backupCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	testcfg.BuildGitalySSH(t, backupCfg)
	testcfg.BuildGitalyHooks(t, backupCfg)

	primary := config.Node{
		Storage: primaryCfg.Storages[0].Name,
		Address: primaryAddr,
	}

	secondary := config.Node{
		Storage: backupCfg.Storages[0].Name,
		Address: backupAddr,
	}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					&primary,
					&secondary,
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(ctx)

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewPostgresReplicationEventQueue(testdb.New(t)))

	// this job exists to verify that replication works
	okJob := datastore.ReplicationJob{
		RepositoryID:      1,
		Change:            datastore.UpdateRepo,
		RelativePath:      testRepo.RelativePath,
		TargetNodeStorage: secondary.Storage,
		SourceNodeStorage: primary.Storage,
		VirtualStorage:    "praefect",
	}
	event1, err := queueInterceptor.Enqueue(ctx, datastore.ReplicationEvent{Job: okJob})
	require.NoError(t, err)
	require.Equal(t, uint64(1), event1.ID)

	// this job checks flow for replication event that fails
	failJob := okJob
	failJob.Change = "invalid-operation"
	event2, err := queueInterceptor.Enqueue(ctx, datastore.ReplicationEvent{Job: failJob})
	require.NoError(t, err)
	require.Equal(t, uint64(2), event2.ID)

	logEntry := testhelper.NewDiscardingLogEntry(t)

	nodeMgr, err := nodes.NewManager(logEntry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)
	defer nodeMgr.Stop()

	db := testdb.New(t)
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
	require.NoError(t, rs.CreateRepository(ctx, okJob.RepositoryID, okJob.VirtualStorage, okJob.RelativePath, okJob.RelativePath, okJob.SourceNodeStorage, nil, nil, true, false))

	replMgr := NewReplMgr(
		logEntry,
		conf.StorageNames(),
		queueInterceptor,
		rs,
		nodeMgr,
		NodeSetFromNodeManager(nodeMgr),
	)
	replMgrDone := startProcessBacklog(ctx, replMgr)

	require.NoError(t, queueInterceptor.Wait(time.Minute, func(i *datastore.ReplicationEventQueueInterceptor) bool {
		return len(i.GetAcknowledgeResult()) == 4
	}))
	cancel()
	<-replMgrDone

	var dequeueCalledEffectively int
	for _, res := range queueInterceptor.GetDequeuedResult() {
		if len(res) > 0 {
			dequeueCalledEffectively++
		}
	}
	require.Equal(t, 3, dequeueCalledEffectively, "expected 1 deque to get [okJob, failJob] and 2 more for [failJob] only")

	expAcks := map[datastore.JobState][]uint64{
		datastore.JobStateFailed:    {2, 2},
		datastore.JobStateDead:      {2},
		datastore.JobStateCompleted: {1},
	}
	acks := map[datastore.JobState][]uint64{}
	for _, ack := range queueInterceptor.GetAcknowledge() {
		acks[ack.State] = append(acks[ack.State], ack.IDs...)
	}
	require.Equal(t, expAcks, acks)
}

func TestProcessBacklog_Success(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cancel := context.WithCancel(ctx)

	primaryCfg, testRepo, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages("primary"))
	primaryCfg.SocketPath = testserver.RunGitalyServer(t, primaryCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	testcfg.BuildGitalySSH(t, primaryCfg)
	testcfg.BuildGitalyHooks(t, primaryCfg)

	backupCfg, _, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages("backup"))
	backupCfg.SocketPath = testserver.RunGitalyServer(t, backupCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	testcfg.BuildGitalySSH(t, backupCfg)
	testcfg.BuildGitalyHooks(t, backupCfg)

	primary := config.Node{
		Storage: primaryCfg.Storages[0].Name,
		Address: primaryCfg.SocketPath,
	}

	secondary := config.Node{
		Storage: backupCfg.Storages[0].Name,
		Address: backupCfg.SocketPath,
	}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "virtual",
				Nodes: []*config.Node{
					&primary,
					&secondary,
				},
			},
		},
	}

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewPostgresReplicationEventQueue(testdb.New(t)))
	queueInterceptor.OnAcknowledge(func(ctx context.Context, state datastore.JobState, ids []uint64, queue datastore.ReplicationEventQueue) ([]uint64, error) {
		ackIDs, err := queue.Acknowledge(ctx, state, ids)
		if len(ids) > 0 {
			assert.Equal(t, datastore.JobStateCompleted, state, "no fails expected")
			assert.Equal(t, []uint64{1, 3, 4}, ids, "all jobs must be processed at once")
		}
		return ackIDs, err
	})

	var healthUpdated int32
	queueInterceptor.OnStartHealthUpdate(func(ctx context.Context, trigger <-chan time.Time, events []datastore.ReplicationEvent) error {
		assert.Len(t, events, 3)
		atomic.AddInt32(&healthUpdated, 1)
		return nil
	})

	// Update replication job
	eventType1 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			RepositoryID:      1,
			Change:            datastore.UpdateRepo,
			RelativePath:      testRepo.GetRelativePath(),
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
			VirtualStorage:    conf.VirtualStorages[0].Name,
		},
	}

	_, err := queueInterceptor.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	_, err = queueInterceptor.Enqueue(ctx, eventType1)
	require.NoError(t, err)

	renameTo1 := filepath.Join(testRepo.GetRelativePath(), "..", filepath.Base(testRepo.GetRelativePath())+"-mv1")
	fullNewPath1 := filepath.Join(backupCfg.Storages[0].Path, renameTo1)

	renameTo2 := filepath.Join(renameTo1, "..", filepath.Base(testRepo.GetRelativePath())+"-mv2")
	fullNewPath2 := filepath.Join(backupCfg.Storages[0].Path, renameTo2)

	// Rename replication job
	eventType2 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.RenameRepo,
			RelativePath:      testRepo.GetRelativePath(),
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
			VirtualStorage:    conf.VirtualStorages[0].Name,
			Params:            datastore.Params{"RelativePath": renameTo1},
		},
	}

	_, err = queueInterceptor.Enqueue(ctx, eventType2)
	require.NoError(t, err)

	// Rename replication job
	eventType3 := datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.RenameRepo,
			RelativePath:      renameTo1,
			TargetNodeStorage: secondary.Storage,
			SourceNodeStorage: primary.Storage,
			VirtualStorage:    conf.VirtualStorages[0].Name,
			Params:            datastore.Params{"RelativePath": renameTo2},
		},
	}
	require.NoError(t, err)

	_, err = queueInterceptor.Enqueue(ctx, eventType3)
	require.NoError(t, err)

	logEntry := testhelper.NewDiscardingLogEntry(t)

	nodeMgr, err := nodes.NewManager(logEntry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)
	defer nodeMgr.Stop()

	db := testdb.New(t)
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
	require.NoError(t, rs.CreateRepository(ctx, eventType1.Job.RepositoryID, eventType1.Job.VirtualStorage, eventType1.Job.VirtualStorage, eventType1.Job.RelativePath, eventType1.Job.SourceNodeStorage, nil, nil, true, false))

	replMgr := NewReplMgr(
		logEntry,
		conf.StorageNames(),
		queueInterceptor,
		rs,
		nodeMgr,
		NodeSetFromNodeManager(nodeMgr),
	)
	replMgrDone := startProcessBacklog(ctx, replMgr)

	require.NoError(t, queueInterceptor.Wait(time.Minute, func(i *datastore.ReplicationEventQueueInterceptor) bool {
		var ids []uint64
		for _, params := range i.GetAcknowledge() {
			ids = append(ids, params.IDs...)
		}
		return len(ids) == 3
	}))
	cancel()
	<-replMgrDone

	require.NoDirExists(t, fullNewPath1, "repository must be moved from %q to the new location", fullNewPath1)
	require.True(t, storage.IsGitDirectory(fullNewPath2), "repository must exist at new last RenameRepository location")
}

func TestReplMgrProcessBacklog_OnlyHealthyNodes(t *testing.T) {
	t.Parallel()
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{Storage: "node-1"},
					{Storage: "node-2"},
					{Storage: "node-3"},
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(testhelper.Context(t))

	var mtx sync.Mutex
	expStorages := map[string]bool{conf.VirtualStorages[0].Nodes[0].Storage: true, conf.VirtualStorages[0].Nodes[2].Storage: true}
	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewPostgresReplicationEventQueue(testdb.New(t)))
	queueInterceptor.OnDequeue(func(_ context.Context, virtualStorageName string, storageName string, _ int, _ datastore.ReplicationEventQueue) ([]datastore.ReplicationEvent, error) {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			mtx.Lock()
			defer mtx.Unlock()
			assert.Equal(t, conf.VirtualStorages[0].Name, virtualStorageName)
			assert.True(t, expStorages[storageName], storageName, storageName)
			delete(expStorages, storageName)
			if len(expStorages) == 0 {
				cancel()
			}
			return nil, nil
		}
	})

	virtualStorage := conf.VirtualStorages[0].Name
	node1 := Node{Storage: conf.VirtualStorages[0].Nodes[0].Storage}
	node2 := Node{Storage: conf.VirtualStorages[0].Nodes[1].Storage}
	node3 := Node{Storage: conf.VirtualStorages[0].Nodes[2].Storage}

	replMgr := NewReplMgr(
		testhelper.NewDiscardingLogEntry(t),
		conf.StorageNames(),
		queueInterceptor,
		nil,
		StaticHealthChecker{virtualStorage: {node1.Storage, node3.Storage}},
		NodeSet{
			virtualStorage: {
				node1.Storage: node1,
				node2.Storage: node2,
				node3.Storage: node3,
			},
		},
	)
	replMgrDone := startProcessBacklog(ctx, replMgr)

	select {
	case <-ctx.Done():
		// completed by scenario
	case <-time.After(30 * time.Second):
		// strongly depends on the processing capacity
		t.Fatal("time limit expired for job to complete")
	}
	<-replMgrDone
}

type mockReplicator struct {
	Replicator
	ReplicateFunc func(ctx context.Context, event datastore.ReplicationEvent, source, target *grpc.ClientConn) error
}

func (m mockReplicator) Replicate(ctx context.Context, event datastore.ReplicationEvent, source, target *grpc.ClientConn) error {
	return m.ReplicateFunc(ctx, event, source, target)
}

func TestProcessBacklog_ReplicatesToReadOnlyPrimary(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	const virtualStorage = "virtal-storage"
	const primaryStorage = "storage-1"
	const secondaryStorage = "storage-2"
	const repositoryID = 1

	primaryConn := &grpc.ClientConn{}
	secondaryConn := &grpc.ClientConn{}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorage,
				Nodes: []*config.Node{
					{Storage: primaryStorage},
					{Storage: secondaryStorage},
				},
			},
		},
	}

	queue := datastore.NewPostgresReplicationEventQueue(testdb.New(t))
	_, err := queue.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			RepositoryID:      1,
			Change:            datastore.UpdateRepo,
			RelativePath:      "ignored",
			TargetNodeStorage: primaryStorage,
			SourceNodeStorage: secondaryStorage,
			VirtualStorage:    virtualStorage,
		},
	})
	require.NoError(t, err)

	db := testdb.New(t)
	rs := datastore.NewPostgresRepositoryStore(db, conf.StorageNames())
	require.NoError(t, rs.CreateRepository(ctx, repositoryID, virtualStorage, "ignored", "ignored", primaryStorage, []string{secondaryStorage}, nil, true, false))

	replMgr := NewReplMgr(
		testhelper.NewDiscardingLogEntry(t),
		conf.StorageNames(),
		queue,
		rs,
		StaticHealthChecker{virtualStorage: {primaryStorage, secondaryStorage}},
		NodeSet{virtualStorage: {
			primaryStorage:   {Storage: primaryStorage, Connection: primaryConn},
			secondaryStorage: {Storage: secondaryStorage, Connection: secondaryConn},
		}},
	)

	processed := make(chan struct{})
	replMgr.replicator = mockReplicator{
		ReplicateFunc: func(ctx context.Context, event datastore.ReplicationEvent, source, target *grpc.ClientConn) error {
			require.True(t, primaryConn == target)
			require.True(t, secondaryConn == source)
			close(processed)
			return nil
		},
	}
	replMgrDone := startProcessBacklog(ctx, replMgr)
	select {
	case <-processed:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatalf("replication job targeting read-only primary was not processed before timeout")
	}
	<-replMgrDone
}

func TestBackoffFactory(t *testing.T) {
	start := 1 * time.Microsecond
	max := 6 * time.Microsecond
	expectedBackoffs := []time.Duration{
		1 * time.Microsecond,
		2 * time.Microsecond,
		4 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
		6 * time.Microsecond,
	}
	b, reset := ExpBackoffFactory{Start: start, Max: max}.Create()
	for _, expectedBackoff := range expectedBackoffs {
		require.Equal(t, expectedBackoff, b())
	}

	reset()
	require.Equal(t, start, b())
}

func newRepositoryClient(t *testing.T, serverSocketPath, token string) gitalypb.RepositoryServiceClient {
	t.Helper()

	conn, err := grpc.Dial(
		serverSocketPath,
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return gitalypb.NewRepositoryServiceClient(conn)
}

func TestSubtractUint64(t *testing.T) {
	testCases := []struct {
		desc  string
		left  []uint64
		right []uint64
		exp   []uint64
	}{
		{desc: "empty left", left: nil, right: []uint64{1, 2}, exp: nil},
		{desc: "empty right", left: []uint64{1, 2}, right: []uint64{}, exp: []uint64{1, 2}},
		{desc: "some exists", left: []uint64{1, 2, 3, 4, 5}, right: []uint64{2, 4, 5}, exp: []uint64{1, 3}},
		{desc: "nothing exists", left: []uint64{10, 20}, right: []uint64{100, 200}, exp: []uint64{10, 20}},
		{desc: "duplicates exists", left: []uint64{1, 1, 2, 3, 3, 4, 4, 5}, right: []uint64{3, 4, 4, 5}, exp: []uint64{1, 1, 2}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			require.Equal(t, testCase.exp, subtractUint64(testCase.left, testCase.right))
		})
	}
}

func TestReplMgr_ProcessStale(t *testing.T) {
	logger := testhelper.NewDiscardingLogger(t)
	hook := test.NewLocal(logger)

	queue := datastore.NewReplicationEventQueueInterceptor(nil)
	mgr := NewReplMgr(logger.WithField("test", t.Name()), nil, queue, datastore.MockRepositoryStore{}, nil, nil)
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	const iterations = 3
	var counter int
	queue.OnAcknowledgeStale(func(context.Context, time.Duration) (int64, error) {
		counter++
		if counter >= iterations {
			cancel()
			return 0, assert.AnError
		}
		return 0, nil
	})

	ticker := helper.NewManualTicker()

	done := mgr.ProcessStale(ctx, ticker, time.Second)
	for i := 0; i < iterations; i++ {
		ticker.Tick()
	}
	<-done

	require.Equal(t, iterations, counter)
	require.Len(t, hook.AllEntries(), 1)
	require.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
	require.Equal(t, "background periodical acknowledgement for stale replication jobs", hook.LastEntry().Message)
	require.Equal(t, "replication_manager", hook.LastEntry().Data["component"])
	require.Equal(t, assert.AnError, hook.LastEntry().Data["error"])
}
