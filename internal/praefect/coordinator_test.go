package praefect

import (
	"context"
	"crypto/sha1"
	"errors"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	praefect_metadata "gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var testLogger = logrus.New()

func init() {
	testLogger.SetOutput(ioutil.Discard)
}

func TestSecondaryRotation(t *testing.T) {
	t.Skip("secondary rotation will change with the new data model")
}

func TestStreamDirectorReadOnlyEnforcement(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		readOnly bool
	}{
		{desc: "writable", readOnly: false},
		{desc: "read-only", readOnly: true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			const (
				virtualStorage = "test-virtual-storage"
				relativePath   = "test-repository"
				storage        = "test-storage"
			)
			conf := config.Config{
				VirtualStorages: []*config.VirtualStorage{
					&config.VirtualStorage{
						Name: virtualStorage,
						Nodes: []*config.Node{
							&config.Node{
								Address: "tcp://gitaly-primary.example.com",
								Storage: storage,
							},
						},
					},
				},
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			rs := datastore.MockRepositoryStore{
				GetConsistentStoragesFunc: func(context.Context, string, string) (map[string]struct{}, error) {
					if tc.readOnly {
						return map[string]struct{}{storage + "-other": {}}, nil
					}
					return map[string]struct{}{storage: {}}, nil
				},
			}

			coordinator := NewCoordinator(
				datastore.NewMemoryReplicationEventQueue(conf),
				rs,
				NewNodeManagerRouter(&nodes.MockManager{GetShardFunc: func(vs string) (nodes.Shard, error) {
					require.Equal(t, virtualStorage, vs)
					return nodes.Shard{
						Primary: &nodes.MockNode{GetStorageMethod: func() string {
							return storage
						}},
					}, nil
				}}, rs),
				transactions.NewManager(conf),
				conf,
				protoregistry.GitalyProtoPreregistered,
			)

			frame, err := proto.Marshal(&gitalypb.CleanupRequest{Repository: &gitalypb.Repository{
				StorageName:  virtualStorage,
				RelativePath: relativePath,
			}})
			require.NoError(t, err)

			_, err = coordinator.StreamDirector(ctx, "/gitaly.RepositoryService/Cleanup", &mockPeeker{frame: frame})
			if tc.readOnly {
				require.Equal(t, ErrRepositoryReadOnly, err)
				testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestStreamDirectorMutator(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, _ := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	primaryNode := &config.Node{Address: primaryAddress, Storage: "praefect-internal-1"}
	secondaryNode := &config.Node{Address: secondaryAddress, Storage: "praefect-internal-2"}
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name:  "praefect",
				Nodes: []*config.Node{primaryNode, secondaryNode},
			},
		},
	}

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)

	txMgr := transactions.NewManager(conf)
	rs := datastore.MockRepositoryStore{}

	coordinator := NewCoordinator(
		queueInterceptor,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	frame, err := proto.Marshal(&gitalypb.CreateObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
	})
	require.NoError(t, err)

	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/CreateObjectPool"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
	require.True(t, ok)
	require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	replEventWait.Add(1) // expected only one event to be created
	// this call creates new events in the queue and simulates usual flow of the update operation
	streamParams.RequestFinalizer()

	replEventWait.Wait() // wait until event persisted (async operation)
	events, err := queueInterceptor.Dequeue(ctx, "praefect", "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, events, 1)

	expectedEvent := datastore.ReplicationEvent{
		ID:        1,
		State:     datastore.JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect|praefect-internal-2|/path/to/hashed/storage",
		CreatedAt: events[0].CreatedAt,
		UpdatedAt: events[0].UpdatedAt,
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    conf.VirtualStorages[0].Name,
			RelativePath:      targetRepo.RelativePath,
			TargetNodeStorage: secondaryNode.Storage,
			SourceNodeStorage: primaryNode.Storage,
		},
		Meta: datastore.Params{metadatahandler.CorrelationIDKey: "my-correlation-id"},
	}
	require.Equal(t, expectedEvent, events[0], "ensure replication job created by stream director is correct")
}

func TestStreamDirectorMutator_StopTransaction(t *testing.T) {
	socket := testhelper.GetTemporaryGitalySocketFileName(t)
	server, _ := testhelper.NewServerWithHealth(t, socket)
	defer server.Stop()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{Address: "unix://" + socket, Storage: "primary"},
					&config.Node{Address: "unix://" + socket, Storage: "secondary"},
				},
			},
		},
	}

	repo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)

	ctx, cancel := testhelper.Context()
	defer cancel()

	shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
	require.NoError(t, err)

	for _, name := range []string{"primary", "secondary"} {
		node, err := shard.GetNode(name)
		require.NoError(t, err)
		waitNodeToChangeHealthStatus(ctx, t, node, true)
	}

	rs := datastore.MockRepositoryStore{
		GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
			return map[string]struct{}{"primary": {}, "secondary": {}}, nil
		},
	}

	txMgr := transactions.NewManager(conf)

	coordinator := NewCoordinator(
		datastore.NewMemoryReplicationEventQueue(conf),
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	fullMethod := "/gitaly.SmartHTTPService/PostReceivePack"

	frame, err := proto.Marshal(&gitalypb.PostReceivePackRequest{
		Repository: &repo,
	})
	require.NoError(t, err)
	peeker := &mockPeeker{frame}

	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)

	transaction, err := praefect_metadata.TransactionFromContext(streamParams.Primary().Ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var syncWG sync.WaitGroup

	wg.Add(2)
	syncWG.Add(2)

	go func() {
		defer wg.Done()

		vote := sha1.Sum([]byte("vote"))
		err := txMgr.VoteTransaction(ctx, transaction.ID, "primary", vote[:])
		require.NoError(t, err)

		// Assure that at least one vote was agreed on.
		syncWG.Done()
		syncWG.Wait()

		require.NoError(t, txMgr.StopTransaction(ctx, transaction.ID))
	}()

	go func() {
		defer wg.Done()

		vote := sha1.Sum([]byte("vote"))
		err := txMgr.VoteTransaction(ctx, transaction.ID, "secondary", vote[:])
		require.NoError(t, err)

		// Assure that at least one vote was agreed on.
		syncWG.Done()
		syncWG.Wait()

		err = txMgr.VoteTransaction(ctx, transaction.ID, "secondary", vote[:])
		assert.True(t, errors.Is(err, transactions.ErrTransactionStopped))
	}()

	wg.Wait()

	err = streamParams.RequestFinalizer()
	require.NoError(t, err)
}

func TestStreamDirectorAccessor(t *testing.T) {
	gitalySocket := testhelper.GetTemporaryGitalySocketFileName(t)
	srv, _ := testhelper.NewServerWithHealth(t, gitalySocket)
	defer srv.Stop()

	gitalyAddress := "unix://" + gitalySocket
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Address: gitalyAddress,
						Storage: "praefect-internal-1",
					},
				},
			},
		},
	}

	queue := datastore.NewMemoryReplicationEventQueue(conf)

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx = featureflag.IncomingCtxWithDisabledFeatureFlag(ctx, featureflag.DistributedReads)

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Minute)

	txMgr := transactions.NewManager(conf)
	rs := datastore.MockRepositoryStore{}

	coordinator := NewCoordinator(
		queue,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
	require.NoError(t, err)

	fullMethod := "/gitaly.RefService/FindAllBranches"

	peeker := &mockPeeker{frame: frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, gitalyAddress, streamParams.Primary().Conn.Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
	require.True(t, ok)
	require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)
	require.Equal(t, protoregistry.ScopeRepository, mi.Scope, "method must be repository scoped")
	require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

	m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")
}

func TestCoordinatorStreamDirector_distributesReads(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, healthSrv := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryNodeConf := config.Node{
		Address: "unix://" + gitalySocket0,
		Storage: "gitaly-1",
	}

	secondaryNodeConf := config.Node{
		Address: "unix://" + gitalySocket1,
		Storage: "gitaly-2",
	}
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name:  "praefect",
				Nodes: []*config.Node{&primaryNodeConf, &secondaryNodeConf},
			},
		},
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: "local",
		},
	}

	queue := datastore.NewMemoryReplicationEventQueue(conf)

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	repoStore := datastore.MockRepositoryStore{
		GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
			return map[string]struct{}{primaryNodeConf.Storage: {}, secondaryNodeConf.Storage: {}}, nil
		},
	}

	sp := datastore.NewDirectStorageProvider(repoStore)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, sp, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Minute)

	txMgr := transactions.NewManager(conf)

	coordinator := NewCoordinator(
		queue,
		repoStore,
		NewNodeManagerRouter(nodeMgr, repoStore),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	t.Run("forwards accessor operations", func(t *testing.T) {
		var primaryChosen int
		var secondaryChosen int

		for i := 0; i < 16; i++ {
			frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
			require.NoError(t, err)

			fullMethod := "/gitaly.RefService/FindAllBranches"

			peeker := &mockPeeker{frame: frame}

			streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
			require.NoError(t, err)
			require.Contains(t, []string{primaryNodeConf.Address, secondaryNodeConf.Address}, streamParams.Primary().Conn.Target(), "must be redirected to primary or secondary")

			var nodeConf config.Node
			switch streamParams.Primary().Conn.Target() {
			case primaryNodeConf.Address:
				nodeConf = primaryNodeConf
				primaryChosen++
			case secondaryNodeConf.Address:
				nodeConf = secondaryNodeConf
				secondaryChosen++
			}

			md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
			require.True(t, ok)
			require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

			mi, err := coordinator.registry.LookupMethod(fullMethod)
			require.NoError(t, err)
			require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

			m, err := protoMessage(mi, streamParams.Primary().Msg)
			require.NoError(t, err)

			rewrittenTargetRepo, err := mi.TargetRepo(m)
			require.NoError(t, err)
			require.Equal(t, nodeConf.Storage, rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
		}

		require.NotZero(t, primaryChosen, "primary should have been chosen at least once")
		require.NotZero(t, secondaryChosen, "secondary should have been chosen at least once")
	})

	t.Run("forwards accessor operations only to healthy nodes", func(t *testing.T) {
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
		require.NoError(t, err)

		gitaly1, err := shard.GetNode(secondaryNodeConf.Storage)
		require.NoError(t, err)
		waitNodeToChangeHealthStatus(ctx, t, gitaly1, false)
		defer func() {
			healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			waitNodeToChangeHealthStatus(ctx, t, gitaly1, true)
		}()

		frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
		require.NoError(t, err)

		fullMethod := "/gitaly.RefService/FindAllBranches"

		peeker := &mockPeeker{frame: frame}
		streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
		require.NoError(t, err)
		require.Equal(t, primaryNodeConf.Address, streamParams.Primary().Conn.Target(), "must be redirected to primary")

		md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
		require.True(t, ok)
		require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

		mi, err := coordinator.registry.LookupMethod(fullMethod)
		require.NoError(t, err)
		require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

		m, err := protoMessage(mi, streamParams.Primary().Msg)
		require.NoError(t, err)

		rewrittenTargetRepo, err := mi.TargetRepo(m)
		require.NoError(t, err)
		require.Equal(t, "gitaly-1", rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
	})

	t.Run("doesn't forward mutator operations", func(t *testing.T) {
		frame, err := proto.Marshal(&gitalypb.UserUpdateBranchRequest{Repository: &targetRepo})
		require.NoError(t, err)

		fullMethod := "/gitaly.OperationService/UserUpdateBranch"

		peeker := &mockPeeker{frame: frame}
		streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
		require.NoError(t, err)
		require.Equal(t, primaryNodeConf.Address, streamParams.Primary().Conn.Target(), "must be redirected to primary")

		md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
		require.True(t, ok)
		require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

		mi, err := coordinator.registry.LookupMethod(fullMethod)
		require.NoError(t, err)
		require.Equal(t, protoregistry.OpMutator, mi.Operation, "method must be a mutator")

		m, err := protoMessage(mi, streamParams.Primary().Msg)
		require.NoError(t, err)

		rewrittenTargetRepo, err := mi.TargetRepo(m)
		require.NoError(t, err)
		require.Equal(t, "gitaly-1", rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
	})
}

func TestStreamDirector_repo_creation(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, _ := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	primaryNode := &config.Node{Address: primaryAddress, Storage: "praefect-internal-1"}
	secondaryNode := &config.Node{Address: secondaryAddress, Storage: "praefect-internal-2"}
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name:  "praefect",
				Nodes: []*config.Node{primaryNode, secondaryNode},
			},
		},
	}

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	rewrittenStorage := primaryNode.Storage
	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)

	txMgr := transactions.NewManager(conf)

	var createRepositoryCalled int64
	rs := datastore.MockRepositoryStore{
		CreateRepositoryFunc: func(ctx context.Context, virtualStorage, relativePath, primary string, storePrimary bool) error {
			atomic.AddInt64(&createRepositoryCalled, 1)
			assert.Equal(t, targetRepo.StorageName, virtualStorage)
			assert.Equal(t, targetRepo.RelativePath, relativePath)
			assert.Equal(t, rewrittenStorage, primary)
			assert.False(t, storePrimary)
			return nil
		},
	}

	coordinator := NewCoordinator(
		queueInterceptor,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	frame, err := proto.Marshal(&gitalypb.CreateRepositoryRequest{
		Repository: &targetRepo,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.RepositoryService/CreateRepository"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
	require.True(t, ok)
	require.Contains(t, md, praefect_metadata.PraefectMetadataKey)

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, rewrittenStorage, rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	replEventWait.Add(1) // expected only one event to be created
	// this call creates new events in the queue and simulates usual flow of the update operation
	err = streamParams.RequestFinalizer()
	require.NoError(t, err)

	replEventWait.Wait() // wait until event persisted (async operation)
	events, err := queueInterceptor.Dequeue(ctx, "praefect", "praefect-internal-2", 10)
	require.NoError(t, err)
	require.Len(t, events, 1)

	expectedEvent := datastore.ReplicationEvent{
		ID:        1,
		State:     datastore.JobStateInProgress,
		Attempt:   2,
		LockID:    "praefect|praefect-internal-2|/path/to/hashed/storage",
		CreatedAt: events[0].CreatedAt,
		UpdatedAt: events[0].UpdatedAt,
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    conf.VirtualStorages[0].Name,
			RelativePath:      targetRepo.RelativePath,
			TargetNodeStorage: secondaryNode.Storage,
			SourceNodeStorage: primaryNode.Storage,
		},
		Meta: datastore.Params{metadatahandler.CorrelationIDKey: "my-correlation-id"},
	}
	require.Equal(t, expectedEvent, events[0], "ensure replication job created by stream director is correct")
	require.EqualValues(t, 1, atomic.LoadInt64(&createRepositoryCalled), "ensure CreateRepository is called on datastore")
}

func waitNodeToChangeHealthStatus(ctx context.Context, t *testing.T, node nodes.Node, health bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	for node.IsHealthy() != health {
		_, err := node.CheckHealth(ctx)
		require.NoError(t, err)
	}
}

type mockPeeker struct {
	frame []byte
}

func (m *mockPeeker) Peek() ([]byte, error) {
	return m.frame, nil
}

func (m *mockPeeker) Modify(payload []byte) error {
	m.frame = payload

	return nil
}

func TestAbsentCorrelationID(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	_, healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	_, healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
	healthSrv0.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv1.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Address: primaryAddress,
						Storage: "praefect-internal-1",
					},
					&config.Node{
						Address: secondaryAddress,
						Storage: "praefect-internal-2",
					},
				},
			},
		},
	}

	var replEventWait sync.WaitGroup

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		defer replEventWait.Done()
		return queue.Enqueue(ctx, event)
	})

	targetRepo := gitalypb.Repository{
		StorageName:  "praefect",
		RelativePath: "/path/to/hashed/storage",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Hour)

	txMgr := transactions.NewManager(conf)
	rs := datastore.MockRepositoryStore{}

	coordinator := NewCoordinator(
		queueInterceptor,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	frame, err := proto.Marshal(&gitalypb.CreateObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/CreateObjectPool"
	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target())

	replEventWait.Add(1) // expected only one event to be created
	// must be run as it adds replication events to the queue
	streamParams.RequestFinalizer()

	replEventWait.Wait() // wait until event persisted (async operation)
	jobs, err := queueInterceptor.Dequeue(ctx, conf.VirtualStorages[0].Name, conf.VirtualStorages[0].Nodes[1].Storage, 1)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	require.NotZero(t, jobs[0].Meta[metadatahandler.CorrelationIDKey],
		"the coordinator should have generated a random ID")
}

func TestCoordinatorEnqueueFailure(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: "praefect",
				Nodes: []*config.Node{
					&config.Node{
						Address: "unix://woof",
						Storage: "praefect-internal-1",
					},
					&config.Node{
						Address: "unix://meow",
						Storage: "praefect-internal-2",
					}},
			},
		},
	}

	queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	errQ := make(chan error, 1)
	queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		return datastore.ReplicationEvent{}, <-errQ
	})

	ms := &mockSvc{
		repoMutatorUnary: func(context.Context, *mock.RepoRequest) (*empty.Empty, error) {
			return &empty.Empty{}, nil // always succeeds
		},
	}

	r, err := protoregistry.New(mustLoadProtoReg(t))
	require.NoError(t, err)

	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{
		withAnnotations: r,
		withQueue:       queueInterceptor,
		withBackends: withMockBackends(t, map[string]mock.SimpleServiceServer{
			conf.VirtualStorages[0].Nodes[0].Storage: ms,
			conf.VirtualStorages[0].Nodes[1].Storage: ms,
		}),
	})
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	mcli := mock.NewSimpleServiceClient(cc)

	errQ <- nil
	repoReq := &mock.RepoRequest{
		Repo: &gitalypb.Repository{
			RelativePath: "meow",
			StorageName:  conf.VirtualStorages[0].Name,
		},
	}
	_, err = mcli.RepoMutatorUnary(ctx, repoReq)
	require.NoError(t, err)

	expectErrMsg := "enqueue failed"
	errQ <- errors.New(expectErrMsg)
	_, err = mcli.RepoMutatorUnary(ctx, repoReq)
	require.Error(t, err)
	require.Equal(t, err.Error(), "rpc error: code = Unknown desc = enqueue replication event: "+expectErrMsg)
}

func TestStreamDirectorStorageScope(t *testing.T) {
	// stubs health-check requests because nodes.NewManager establishes connection on creation
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, _ := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	primaryGitaly := &config.Node{Address: primaryAddress, Storage: "gitaly-1"}
	secondaryGitaly := &config.Node{Address: secondaryAddress, Storage: "gitaly-2"}
	conf := config.Config{
		Failover: config.Failover{Enabled: true},
		VirtualStorages: []*config.VirtualStorage{{
			Name:  "praefect",
			Nodes: []*config.Node{primaryGitaly, secondaryGitaly},
		}}}

	rs := datastore.MockRepositoryStore{}

	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Second)
	coordinator := NewCoordinator(
		nil,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		nil,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("mutator", func(t *testing.T) {
		fullMethod := "/gitaly.NamespaceService/RemoveNamespace"
		requireScopeOperation(t, coordinator.registry, fullMethod, protoregistry.ScopeStorage, protoregistry.OpMutator)

		frame, err := proto.Marshal(&gitalypb.RemoveNamespaceRequest{
			StorageName: conf.VirtualStorages[0].Name,
			Name:        "stub",
		})
		require.NoError(t, err)

		streamParams, err := coordinator.StreamDirector(ctx, fullMethod, &mockPeeker{frame})
		require.NoError(t, err)

		require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target(), "stream director didn't redirect to gitaly storage")

		rewritten := gitalypb.RemoveNamespaceRequest{}
		require.NoError(t, proto.Unmarshal(streamParams.Primary().Msg, &rewritten))
		require.Equal(t, primaryGitaly.Storage, rewritten.StorageName, "stream director didn't rewrite storage")
	})

	t.Run("accessor", func(t *testing.T) {
		fullMethod := "/gitaly.NamespaceService/NamespaceExists"
		requireScopeOperation(t, coordinator.registry, fullMethod, protoregistry.ScopeStorage, protoregistry.OpAccessor)

		frame, err := proto.Marshal(&gitalypb.NamespaceExistsRequest{
			StorageName: conf.VirtualStorages[0].Name,
			Name:        "stub",
		})
		require.NoError(t, err)

		streamParams, err := coordinator.StreamDirector(ctx, fullMethod, &mockPeeker{frame})
		require.NoError(t, err)

		require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target(), "stream director didn't redirect to gitaly storage")

		rewritten := gitalypb.RemoveNamespaceRequest{}
		require.NoError(t, proto.Unmarshal(streamParams.Primary().Msg, &rewritten))
		require.Equal(t, primaryGitaly.Storage, rewritten.StorageName, "stream director didn't rewrite storage")
	})
}

func TestStreamDirectorStorageScopeError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("no storage provided", func(t *testing.T) {
		mgr := &nodes.MockManager{
			GetShardFunc: func(s string) (nodes.Shard, error) {
				require.FailNow(t, "validation of input was not executed")
				return nodes.Shard{}, assert.AnError
			},
		}

		rs := datastore.MockRepositoryStore{}
		coordinator := NewCoordinator(
			nil,
			rs,
			NewNodeManagerRouter(mgr, rs),
			nil,
			config.Config{},
			protoregistry.GitalyProtoPreregistered,
		)

		frame, err := proto.Marshal(&gitalypb.RemoveNamespaceRequest{StorageName: "", Name: "stub"})
		require.NoError(t, err)

		_, err = coordinator.StreamDirector(ctx, "/gitaly.NamespaceService/RemoveNamespace", &mockPeeker{frame})
		require.Error(t, err)
		result, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, result.Code())
		require.Equal(t, "storage scoped: target storage is invalid", result.Message())
	})

	t.Run("unknown storage provided", func(t *testing.T) {
		mgr := &nodes.MockManager{
			GetShardFunc: func(s string) (nodes.Shard, error) {
				require.Equal(t, "fake", s)
				return nodes.Shard{}, nodes.ErrVirtualStorageNotExist
			},
		}

		rs := datastore.MockRepositoryStore{}
		coordinator := NewCoordinator(
			nil,
			rs,
			NewNodeManagerRouter(mgr, rs),
			nil,
			config.Config{},
			protoregistry.GitalyProtoPreregistered,
		)

		frame, err := proto.Marshal(&gitalypb.RemoveNamespaceRequest{StorageName: "fake", Name: "stub"})
		require.NoError(t, err)

		_, err = coordinator.StreamDirector(ctx, "/gitaly.NamespaceService/RemoveNamespace", &mockPeeker{frame})
		require.Error(t, err)
		result, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, result.Code())
		require.Equal(t, "virtual storage does not exist", result.Message())
	})

	t.Run("primary is not healthy", func(t *testing.T) {
		t.Run("accessor", func(t *testing.T) {
			mgr := &nodes.MockManager{
				GetShardFunc: func(s string) (nodes.Shard, error) {
					require.Equal(t, "fake", s)
					return nodes.Shard{}, nodes.ErrPrimaryNotHealthy
				},
			}

			rs := datastore.MockRepositoryStore{}
			coordinator := NewCoordinator(
				nil,
				rs,
				NewNodeManagerRouter(mgr, rs),
				nil,
				config.Config{},
				protoregistry.GitalyProtoPreregistered,
			)

			fullMethod := "/gitaly.NamespaceService/NamespaceExists"
			requireScopeOperation(t, coordinator.registry, fullMethod, protoregistry.ScopeStorage, protoregistry.OpAccessor)

			frame, err := proto.Marshal(&gitalypb.NamespaceExistsRequest{StorageName: "fake", Name: "stub"})
			require.NoError(t, err)

			_, err = coordinator.StreamDirector(ctx, fullMethod, &mockPeeker{frame})
			require.Error(t, err)
			result, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Internal, result.Code())
			require.Equal(t, `accessor storage scoped: route storage accessor "fake": primary is not healthy`, result.Message())
		})

		t.Run("mutator", func(t *testing.T) {
			mgr := &nodes.MockManager{
				GetShardFunc: func(s string) (nodes.Shard, error) {
					require.Equal(t, "fake", s)
					return nodes.Shard{}, nodes.ErrPrimaryNotHealthy
				},
			}
			rs := datastore.MockRepositoryStore{}
			coordinator := NewCoordinator(
				nil,
				rs,
				NewNodeManagerRouter(mgr, rs),
				nil,
				config.Config{},
				protoregistry.GitalyProtoPreregistered,
			)

			fullMethod := "/gitaly.NamespaceService/RemoveNamespace"
			requireScopeOperation(t, coordinator.registry, fullMethod, protoregistry.ScopeStorage, protoregistry.OpMutator)

			frame, err := proto.Marshal(&gitalypb.RemoveNamespaceRequest{StorageName: "fake", Name: "stub"})
			require.NoError(t, err)

			_, err = coordinator.StreamDirector(ctx, fullMethod, &mockPeeker{frame})
			require.Error(t, err)
			result, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.Internal, result.Code())
			require.Equal(t, `mutator storage scoped: get shard "fake": primary is not healthy`, result.Message())
		})
	})
}

func TestDisabledTransactionsWithFeatureFlag(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		for rpc, enabledFn := range transactionRPCs {
			if enabledFn(ctx) {
				require.Equal(t,
					featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions),
					shouldUseTransaction(ctx, rpc),
				)
				break
			}
		}
	})
}

func requireScopeOperation(t *testing.T, registry *protoregistry.Registry, fullMethod string, scope protoregistry.Scope, op protoregistry.OpType) {
	t.Helper()

	mi, err := registry.LookupMethod(fullMethod)
	require.NoError(t, err)
	require.Equal(t, scope, mi.Scope, "scope doesn't match requested")
	require.Equal(t, op, mi.Operation, "operation type doesn't match requested")
}
