package praefect

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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
	testhelper.NewServerWithHealth(t, gitalySocket0)
	testhelper.NewServerWithHealth(t, gitalySocket1)

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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
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
	require.Contains(t, md, txinfo.PraefectMetadataKey)

	mi, err := coordinator.registry.LookupMethod(fullMethod)
	require.NoError(t, err)

	m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
	require.NoError(t, err)

	rewrittenTargetRepo, err := mi.TargetRepo(m)
	require.NoError(t, err)
	require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

	replEventWait.Add(1) // expected only one event to be created
	// this call creates new events in the queue and simulates usual flow of the update operation
	require.NoError(t, streamParams.RequestFinalizer())

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
	testhelper.NewServerWithHealth(t, socket)

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

	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
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

	txCtx := peer.NewContext(streamParams.Primary().Ctx, &peer.Peer{})
	transaction, err := txinfo.TransactionFromContext(txCtx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var syncWG sync.WaitGroup

	wg.Add(2)
	syncWG.Add(2)

	go func() {
		defer wg.Done()

		vote := voting.VoteFromData([]byte("vote"))
		err := txMgr.VoteTransaction(ctx, transaction.ID, "primary", vote)
		require.NoError(t, err)

		// Assure that at least one vote was agreed on.
		syncWG.Done()
		syncWG.Wait()

		require.NoError(t, txMgr.StopTransaction(ctx, transaction.ID))
	}()

	go func() {
		defer wg.Done()

		vote := voting.VoteFromData([]byte("vote"))
		err := txMgr.VoteTransaction(ctx, transaction.ID, "secondary", vote)
		require.NoError(t, err)

		// Assure that at least one vote was agreed on.
		syncWG.Done()
		syncWG.Wait()

		err = txMgr.VoteTransaction(ctx, transaction.ID, "secondary", vote)
		assert.True(t, errors.Is(err, transactions.ErrTransactionStopped))
	}()

	wg.Wait()

	err = streamParams.RequestFinalizer()
	require.NoError(t, err)
}

type mockRouter struct {
	Router
	routeRepositoryAccessorFunc func(ctx context.Context, virtualStorage, relativePath string, forcePrimary bool) (RouterNode, error)
}

func (m mockRouter) RouteRepositoryAccessor(ctx context.Context, virtualStorage, relativePath string, forcePrimary bool) (RouterNode, error) {
	return m.routeRepositoryAccessorFunc(ctx, virtualStorage, relativePath, forcePrimary)
}

func TestStreamDirectorAccessor(t *testing.T) {
	gitalySocket := testhelper.GetTemporaryGitalySocketFileName(t)
	testhelper.NewServerWithHealth(t, gitalySocket)

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

	entry := testhelper.DiscardTestEntry(t)
	rs := datastore.MockRepositoryStore{}

	nodeMgr, err := nodes.NewManager(entry, conf, nil, rs, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Minute)

	txMgr := transactions.NewManager(conf)

	for _, tc := range []struct {
		desc   string
		router Router
		error  error
	}{
		{
			desc:   "success",
			router: NewNodeManagerRouter(nodeMgr, rs),
		},
		{
			desc: "repository not found",
			router: mockRouter{
				routeRepositoryAccessorFunc: func(_ context.Context, virtualStorage, relativePath string, _ bool) (RouterNode, error) {
					return RouterNode{}, commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
				},
			},
			error: helper.ErrNotFound(commonerr.NewRepositoryNotFoundError(targetRepo.StorageName, targetRepo.RelativePath)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			coordinator := NewCoordinator(
				queue,
				rs,
				tc.router,
				txMgr,
				conf,
				protoregistry.GitalyProtoPreregistered,
			)

			frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
			require.NoError(t, err)

			fullMethod := "/gitaly.RefService/FindAllBranches"

			peeker := &mockPeeker{frame: frame}
			streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
			if tc.error != nil {
				require.Equal(t, tc.error, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, gitalyAddress, streamParams.Primary().Conn.Target())

			md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
			require.True(t, ok)
			require.Contains(t, md, txinfo.PraefectMetadataKey)

			mi, err := coordinator.registry.LookupMethod(fullMethod)
			require.NoError(t, err)
			require.Equal(t, protoregistry.ScopeRepository, mi.Scope, "method must be repository scoped")
			require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

			m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
			require.NoError(t, err)

			rewrittenTargetRepo, err := mi.TargetRepo(m)
			require.NoError(t, err)
			require.Equal(t, "praefect-internal-1", rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")
		})
	}
}

func TestCoordinatorStreamDirector_distributesReads(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t), testhelper.GetTemporaryGitalySocketFileName(t)
	primaryHealthSrv := testhelper.NewServerWithHealth(t, gitalySocket0)
	healthSrv := testhelper.NewServerWithHealth(t, gitalySocket1)

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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, repoStore, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
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
			require.Contains(t, md, txinfo.PraefectMetadataKey)

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

	t.Run("forwards accessor to primary if force-routing", func(t *testing.T) {
		var primaryChosen int
		var secondaryChosen int

		for i := 0; i < 16; i++ {
			frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
			require.NoError(t, err)

			fullMethod := "/gitaly.RefService/FindAllBranches"

			peeker := &mockPeeker{frame: frame}

			ctx := correlation.ContextWithCorrelation(ctx, "my-correlation-id")
			ctx = testhelper.MergeIncomingMetadata(ctx, metadata.Pairs(routeRepositoryAccessorPolicy, routeRepositoryAccessorPolicyPrimaryOnly))

			streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
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
			require.Contains(t, md, txinfo.PraefectMetadataKey)

			mi, err := coordinator.registry.LookupMethod(fullMethod)
			require.NoError(t, err)
			require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

			m, err := protoMessage(mi, streamParams.Primary().Msg)
			require.NoError(t, err)

			rewrittenTargetRepo, err := mi.TargetRepo(m)
			require.NoError(t, err)
			require.Equal(t, nodeConf.Storage, rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
		}

		require.Equal(t, 16, primaryChosen, "primary should have always been chosen")
		require.Zero(t, secondaryChosen, "secondary should never have been chosen")
	})

	t.Run("forwards accessor to primary for primary-only RPCs", func(t *testing.T) {
		var primaryChosen int
		var secondaryChosen int

		for i := 0; i < 16; i++ {
			frame, err := proto.Marshal(&gitalypb.GetObjectDirectorySizeRequest{Repository: &targetRepo})
			require.NoError(t, err)

			fullMethod := "/gitaly.RepositoryService/GetObjectDirectorySize"

			peeker := &mockPeeker{frame: frame}

			ctx, cancel := testhelper.Context()
			defer cancel()

			streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
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
			require.Contains(t, md, txinfo.PraefectMetadataKey)

			mi, err := coordinator.registry.LookupMethod(fullMethod)
			require.NoError(t, err)
			require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

			m, err := protoMessage(mi, streamParams.Primary().Msg)
			require.NoError(t, err)

			rewrittenTargetRepo, err := mi.TargetRepo(m)
			require.NoError(t, err)
			require.Equal(t, nodeConf.Storage, rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
		}

		require.Equal(t, 16, primaryChosen, "primary should have always been chosen")
		require.Zero(t, secondaryChosen, "secondary should never have been chosen")
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
		require.Contains(t, md, txinfo.PraefectMetadataKey)

		mi, err := coordinator.registry.LookupMethod(fullMethod)
		require.NoError(t, err)
		require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

		m, err := protoMessage(mi, streamParams.Primary().Msg)
		require.NoError(t, err)

		rewrittenTargetRepo, err := mi.TargetRepo(m)
		require.NoError(t, err)
		require.Equal(t, "gitaly-1", rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
	})

	t.Run("fails if force-routing to unhealthy primary", func(t *testing.T) {
		primaryHealthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
		require.NoError(t, err)

		primaryGitaly, err := shard.GetNode(primaryNodeConf.Storage)
		require.NoError(t, err)
		waitNodeToChangeHealthStatus(ctx, t, primaryGitaly, false)
		defer func() {
			primaryHealthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
			waitNodeToChangeHealthStatus(ctx, t, primaryGitaly, true)
		}()

		frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
		require.NoError(t, err)

		fullMethod := "/gitaly.RefService/FindAllBranches"

		ctx := correlation.ContextWithCorrelation(ctx, "my-correlation-id")
		ctx = testhelper.MergeIncomingMetadata(ctx, metadata.Pairs(routeRepositoryAccessorPolicy, routeRepositoryAccessorPolicyPrimaryOnly))

		peeker := &mockPeeker{frame: frame}
		_, err = coordinator.StreamDirector(ctx, fullMethod, peeker)
		require.True(t, errors.Is(err, nodes.ErrPrimaryNotHealthy))
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
		require.Contains(t, md, txinfo.PraefectMetadataKey)

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
	for _, tc := range []struct {
		desc              string
		electionStrategy  config.ElectionStrategy
		replicationFactor int
		primaryStored     bool
		assignmentsStored bool
	}{
		{
			desc:              "virtual storage scoped primaries",
			electionStrategy:  config.ElectionStrategySQL,
			replicationFactor: 3, // assignments are not set when not using repository specific primaries
			primaryStored:     false,
			assignmentsStored: false,
		},
		{
			desc:              "repository specific primaries without variable replication factor",
			electionStrategy:  config.ElectionStrategyPerRepository,
			primaryStored:     true,
			assignmentsStored: false,
		},
		{
			desc:              "repository specific primaries with variable replication factor",
			electionStrategy:  config.ElectionStrategyPerRepository,
			replicationFactor: 3,
			primaryStored:     true,
			assignmentsStored: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			primaryNode := &config.Node{Storage: "praefect-internal-1"}
			healthySecondaryNode := &config.Node{Storage: "praefect-internal-2"}
			unhealthySecondaryNode := &config.Node{Storage: "praefect-internal-3"}
			conf := config.Config{
				Failover: config.Failover{ElectionStrategy: tc.electionStrategy},
				VirtualStorages: []*config.VirtualStorage{
					&config.VirtualStorage{
						Name:                     "praefect",
						DefaultReplicationFactor: tc.replicationFactor,
						Nodes:                    []*config.Node{primaryNode, healthySecondaryNode, unhealthySecondaryNode},
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

			var createRepositoryCalled int64
			rs := datastore.MockRepositoryStore{
				CreateRepositoryFunc: func(ctx context.Context, virtualStorage, relativePath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error {
					atomic.AddInt64(&createRepositoryCalled, 1)
					assert.Equal(t, targetRepo.StorageName, virtualStorage)
					assert.Equal(t, targetRepo.RelativePath, relativePath)
					assert.Equal(t, rewrittenStorage, primary)
					assert.Equal(t, []string{healthySecondaryNode.Storage}, updatedSecondaries)
					assert.Equal(t, []string{unhealthySecondaryNode.Storage}, outdatedSecondaries)
					assert.Equal(t, tc.primaryStored, storePrimary)
					assert.Equal(t, tc.assignmentsStored, storeAssignments)
					return nil
				},
			}

			var router Router
			var primaryConnPointer string
			var secondaryConnPointers []string
			switch tc.electionStrategy {
			case config.ElectionStrategySQL:
				gitalySocket0 := testhelper.GetTemporaryGitalySocketFileName(t)
				gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(t)
				gitalySocket2 := testhelper.GetTemporaryGitalySocketFileName(t)
				testhelper.NewServerWithHealth(t, gitalySocket0)
				testhelper.NewServerWithHealth(t, gitalySocket1)
				healthSrv2 := testhelper.NewServerWithHealth(t, gitalySocket2)
				healthSrv2.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

				primaryNode.Address = "unix://" + gitalySocket0
				healthySecondaryNode.Address = "unix://" + gitalySocket1
				unhealthySecondaryNode.Address = "unix://" + gitalySocket2

				nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
				require.NoError(t, err)
				nodeMgr.Start(0, time.Hour)

				router = NewNodeManagerRouter(nodeMgr, rs)
				for _, node := range nodeMgr.Nodes()["praefect"] {
					if node.GetStorage() == primaryNode.Storage {
						primaryConnPointer = fmt.Sprintf("%p", node.GetConnection())
						continue
					}

					if node.GetStorage() == healthySecondaryNode.Storage {
						secondaryConnPointers = []string{fmt.Sprintf("%p", node.GetConnection())}
					}
				}
			case config.ElectionStrategyPerRepository:
				conns := Connections{
					"praefect": {
						primaryNode.Storage:            &grpc.ClientConn{},
						healthySecondaryNode.Storage:   &grpc.ClientConn{},
						unhealthySecondaryNode.Storage: &grpc.ClientConn{},
					},
				}
				primaryConnPointer = fmt.Sprintf("%p", conns["praefect"][primaryNode.Storage])
				secondaryConnPointers = []string{fmt.Sprintf("%p", conns["praefect"][healthySecondaryNode.Storage])}
				router = NewPerRepositoryRouter(
					conns,
					nil,
					StaticHealthChecker{"praefect": {primaryNode.Storage, healthySecondaryNode.Storage}},
					mockRandom{
						intnFunc: func(n int) int {
							require.Equal(t, n, 2, "number of primary candidates should match the number of healthy nodes")
							return 0
						},
						shuffleFunc: func(n int, swap func(int, int)) {
							require.Equal(t, n, 2, "number of secondary candidates should match the number of node minus the primary")
						},
					},
					nil,
					nil,
					conf.DefaultReplicationFactors(),
				)
			default:
				t.Fatalf("unexpected election strategy: %q", tc.electionStrategy)
			}

			txMgr := transactions.NewManager(conf)

			coordinator := NewCoordinator(
				queueInterceptor,
				rs,
				router,
				txMgr,
				conf,
				protoregistry.GitalyProtoPreregistered,
			)

			frame, err := proto.Marshal(&gitalypb.CreateRepositoryRequest{
				Repository: &targetRepo,
			})
			require.NoError(t, err)

			fullMethod := "/gitaly.RepositoryService/CreateRepository"

			ctx, cancel := testhelper.Context()
			defer cancel()

			peeker := &mockPeeker{frame}
			streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
			require.NoError(t, err)
			require.Equal(t, primaryConnPointer, fmt.Sprintf("%p", streamParams.Primary().Conn))

			var secondaries []string
			for _, dst := range streamParams.Secondaries() {
				secondaries = append(secondaries, fmt.Sprintf("%p", dst.Conn))
			}
			require.Equal(t, secondaryConnPointers, secondaries, "secondary connections did not match expected")

			md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
			require.True(t, ok)
			require.Contains(t, md, txinfo.PraefectMetadataKey)

			mi, err := coordinator.registry.LookupMethod(fullMethod)
			require.NoError(t, err)

			m, err := mi.UnmarshalRequestProto(streamParams.Primary().Msg)
			require.NoError(t, err)

			rewrittenTargetRepo, err := mi.TargetRepo(m)
			require.NoError(t, err)
			require.Equal(t, rewrittenStorage, rewrittenTargetRepo.GetStorageName(), "stream director should have rewritten the storage name")

			replEventWait.Add(1)

			vote := voting.VoteFromData([]byte{})
			require.NoError(t, txMgr.VoteTransaction(ctx, 1, "praefect-internal-1", vote))
			require.NoError(t, txMgr.VoteTransaction(ctx, 1, "praefect-internal-2", vote))

			// this call creates new events in the queue and simulates usual flow of the update operation
			err = streamParams.RequestFinalizer()
			require.NoError(t, err)

			replEventWait.Wait() // wait until event persisted (async operation)

			var expectedEvents, actualEvents []datastore.ReplicationEvent
			for _, target := range []string{unhealthySecondaryNode.Storage} {
				actual, err := queueInterceptor.Dequeue(ctx, "praefect", target, 10)
				require.NoError(t, err)
				require.Len(t, actual, 1)

				actualEvents = append(actualEvents, actual[0])
				expectedEvents = append(expectedEvents, datastore.ReplicationEvent{
					ID:        actual[0].ID,
					State:     datastore.JobStateInProgress,
					Attempt:   2,
					LockID:    fmt.Sprintf("praefect|%s|/path/to/hashed/storage", target),
					CreatedAt: actual[0].CreatedAt,
					UpdatedAt: actual[0].UpdatedAt,
					Job: datastore.ReplicationJob{
						Change:            datastore.UpdateRepo,
						VirtualStorage:    conf.VirtualStorages[0].Name,
						RelativePath:      targetRepo.RelativePath,
						TargetNodeStorage: target,
						SourceNodeStorage: primaryNode.Storage,
					},
					Meta: datastore.Params{metadatahandler.CorrelationIDKey: "my-correlation-id"},
				})
			}

			require.Equal(t, expectedEvents, actualEvents, "ensure replication job created by stream director is correct")
			require.EqualValues(t, 1, atomic.LoadInt64(&createRepositoryCalled), "ensure CreateRepository is called on datastore")
		})
	}
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
	healthSrv0 := testhelper.NewServerWithHealth(t, gitalySocket0)
	healthSrv1 := testhelper.NewServerWithHealth(t, gitalySocket1)
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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
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
	require.NoError(t, streamParams.RequestFinalizer())

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
	testhelper.NewServerWithHealth(t, gitalySocket0)
	testhelper.NewServerWithHealth(t, gitalySocket1)

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

	nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
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

	t.Run("primary gitaly is not healthy", func(t *testing.T) {
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
			require.Equal(t, `accessor storage scoped: route storage accessor "fake": primary gitaly is not healthy`, result.Message())
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
			require.Equal(t, `mutator storage scoped: get shard "fake": primary gitaly is not healthy`, result.Message())
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

type mockOperationServer struct {
	gitalypb.UnimplementedOperationServiceServer
	t      testing.TB
	wg     *sync.WaitGroup
	err    error
	called bool
}

func (s *mockOperationServer) UserCreateBranch(
	context.Context,
	*gitalypb.UserCreateBranchRequest,
) (*gitalypb.UserCreateBranchResponse, error) {
	// We need to wait for all servers to arrive in this RPC. If we don't it could be that for
	// example the primary arrives quicker than the others and directly errors. This would cause
	// stream cancellation, and if the secondaries didn't yet end up in UserCreateBranch, we
	// wouldn't see the function call.
	s.called = true
	s.wg.Done()
	s.wg.Wait()
	return &gitalypb.UserCreateBranchResponse{}, s.err
}

// TestCoordinator_grpcErrorHandling asserts that we correctly proxy errors in case any of the nodes
// fails. Most importantly, we want to make sure to only ever forward errors from the primary and
// never from the secondaries.
func TestCoordinator_grpcErrorHandling(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	praefectConfig := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			&config.VirtualStorage{
				Name: testhelper.DefaultStorageName,
			},
		},
	}

	type gitalyNode struct {
		mock            *nodes.MockNode
		operationServer *mockOperationServer
	}

	_, repoProto, _ := testcfg.BuildWithRepo(t)

	for _, tc := range []struct {
		desc        string
		errByNode   map[string]error
		expectedErr error
	}{
		{
			desc: "no errors",
		},
		{
			desc: "primary error gets forwarded",
			errByNode: map[string]error{
				"primary": errors.New("foo"),
			},
			expectedErr: status.Error(codes.Unknown, "foo"),
		},
		{
			desc: "secondary error gets ignored",
			errByNode: map[string]error{
				"secondary-1": errors.New("foo"),
			},
		},
		{
			desc: "primary error has precedence",
			errByNode: map[string]error{
				"primary":     errors.New("primary"),
				"secondary-1": errors.New("secondary-1"),
				"secondary-2": errors.New("secondary-2"),
			},
			expectedErr: status.Error(codes.Unknown, "primary"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var wg sync.WaitGroup
			gitalies := make(map[string]gitalyNode)
			for _, gitaly := range []string{"primary", "secondary-1", "secondary-2"} {
				gitaly := gitaly

				cfg := testcfg.Build(t, testcfg.WithStorages(gitaly))
				cfg.ListenAddr = ":0"

				operationServer := &mockOperationServer{
					t:  t,
					wg: &wg,
				}
				addr := testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
					gitalypb.RegisterOperationServiceServer(srv, operationServer)
				})

				conn, err := client.DialContext(ctx, addr, []grpc.DialOption{
					grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
				})
				require.NoError(t, err)
				defer conn.Close()

				gitalies[gitaly] = gitalyNode{
					mock: &nodes.MockNode{
						Conn:             conn,
						Healthy:          true,
						GetStorageMethod: func() string { return gitaly },
					},
					operationServer: operationServer,
				}

				praefectConfig.VirtualStorages[0].Nodes = append(praefectConfig.VirtualStorages[0].Nodes, &config.Node{
					Address: addr,
					Storage: gitaly,
				})
			}

			praefectConn, _, cleanup := runPraefectServer(t, praefectConfig, buildOptions{
				// Set up a mock manager which sets up primary/secondaries and pretends that all nodes are
				// healthy. We need fixed roles and unhealthy nodes will not take part in transactions.
				withNodeMgr: &nodes.MockManager{
					Storage: testhelper.DefaultStorageName,
					GetShardFunc: func(shardName string) (nodes.Shard, error) {
						require.Equal(t, testhelper.DefaultStorageName, shardName)
						return nodes.Shard{
							Primary: gitalies["primary"].mock,
							Secondaries: []nodes.Node{
								gitalies["secondary-1"].mock,
								gitalies["secondary-2"].mock,
							},
						}, nil
					},
				},
				// Set up a mock repsoitory store pretending that all nodes are consistent. Only consistent
				// nodes will take part in transactions.
				withRepoStore: datastore.MockRepositoryStore{
					GetConsistentStoragesFunc: func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
						return map[string]struct{}{"primary": {}, "secondary-1": {}, "secondary-2": {}}, nil
					},
				},
			})
			defer cleanup()

			for name, node := range gitalies {
				wg.Add(1)
				node.operationServer.err = tc.errByNode[name]
				node.operationServer.called = false
			}

			_, err := gitalypb.NewOperationServiceClient(praefectConn).UserCreateBranch(ctx,
				&gitalypb.UserCreateBranchRequest{
					Repository: repoProto,
				})
			require.Equal(t, tc.expectedErr, err)

			for _, node := range gitalies {
				require.True(t, node.operationServer.called, "expected gitaly %q to have been called", node.mock.GetStorage())
			}
		})
	}
}

type mockTransaction struct {
	nodeStates                 map[string]transactions.VoteResult
	subtransactions            int
	didCommitAnySubtransaction bool
}

func (t mockTransaction) ID() uint64 {
	return 0
}

func (t mockTransaction) CountSubtransactions() int {
	return t.subtransactions
}

func (t mockTransaction) DidCommitAnySubtransaction() bool {
	return t.didCommitAnySubtransaction
}

func (t mockTransaction) State() (map[string]transactions.VoteResult, error) {
	return t.nodeStates, nil
}

func TestGetUpdatedAndOutdatedSecondaries(t *testing.T) {
	type node struct {
		name  string
		state transactions.VoteResult
		err   error
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	anyErr := errors.New("arbitrary error")

	for _, tc := range []struct {
		desc                       string
		primary                    node
		secondaries                []node
		replicas                   []string
		subtransactions            int
		didCommitAnySubtransaction bool
		expectedPrimaryDirtied     bool
		expectedOutdated           []string
		expectedUpdated            []string
		expectedMetrics            map[string]int
	}{
		{
			desc: "single committed node",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
		},
		{
			desc: "single failed node",
			primary: node{
				name:  "primary",
				state: transactions.VoteFailed,
			},
			subtransactions: 1,
		},
		{
			desc: "single erred node",
			primary: node{
				name: "primary",
				err:  anyErr,
			},
		},
		{
			desc: "single node without subtransactions",
			primary: node{
				name: "primary",
			},
			subtransactions:        0,
			expectedPrimaryDirtied: true,
		},
		{
			desc: "single successful node with replica",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			replicas:                   []string{"replica"},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"replica"},
			expectedMetrics: map[string]int{
				"outdated": 1,
			},
		},
		{
			desc: "single failing node with replica",
			primary: node{
				name:  "primary",
				state: transactions.VoteFailed,
			},
			replicas:         []string{"replica"},
			subtransactions:  1,
			expectedOutdated: []string{"replica"},
		},
		{
			desc: "single erred node with replica",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
				err:   anyErr,
			},
			replicas:                   []string{"replica"},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"replica"},
			expectedMetrics: map[string]int{
				"outdated": 1,
			},
		},
		{
			desc: "single node without transaction with replica",
			primary: node{
				name: "primary",
			},
			replicas:               []string{"replica"},
			subtransactions:        0,
			expectedPrimaryDirtied: true,
			expectedOutdated:       []string{"replica"},
			expectedMetrics: map[string]int{
				"outdated": 1,
			},
		},
		{
			desc: "multiple committed nodes",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteCommitted},
				{name: "s2", state: transactions.VoteCommitted},
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedUpdated:            []string{"s1", "s2"},
		},
		{
			desc: "multiple committed nodes with primary err",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
				err:   anyErr,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteCommitted},
				{name: "s2", state: transactions.VoteCommitted},
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"s1", "s2"},
			expectedMetrics: map[string]int{
				"primary-failed": 2,
			},
		},
		{
			desc: "multiple committed nodes with secondary err",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteCommitted, err: anyErr},
				{name: "s2", state: transactions.VoteCommitted},
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedUpdated:            []string{"s2"},
			expectedOutdated:           []string{"s1"},
			expectedMetrics: map[string]int{
				"node-failed": 1,
			},
		},
		{
			desc: "partial success",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteFailed},
				{name: "s2", state: transactions.VoteCommitted},
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedUpdated:            []string{"s2"},
			expectedOutdated:           []string{"s1"},
			expectedMetrics: map[string]int{
				"node-not-committed": 1,
			},
		},
		{
			desc: "failure with (impossible) secondary success",
			primary: node{
				name:  "primary",
				state: transactions.VoteFailed,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteFailed},
				{name: "s2", state: transactions.VoteCommitted},
			},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"s1", "s2"},
			expectedMetrics: map[string]int{
				"primary-not-committed": 2,
			},
		},
		{
			desc: "multiple nodes without subtransactions",
			primary: node{
				name:  "primary",
				state: transactions.VoteFailed,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteFailed},
				{name: "s2", state: transactions.VoteCommitted},
			},
			subtransactions:        0,
			expectedPrimaryDirtied: true,
			expectedOutdated:       []string{"s1", "s2"},
			expectedMetrics: map[string]int{
				"no-votes": 2,
			},
		},
		{
			desc: "multiple nodes with replica and partial failures",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteFailed},
				{name: "s2", state: transactions.VoteCommitted},
			},
			replicas:                   []string{"r1", "r2"},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"s1", "r1", "r2"},
			expectedUpdated:            []string{"s2"},
			expectedMetrics: map[string]int{
				"node-not-committed": 1,
				"outdated":           2,
			},
		},
		{
			desc: "multiple nodes with replica and partial err",
			primary: node{
				name:  "primary",
				state: transactions.VoteCommitted,
			},
			secondaries: []node{
				{name: "s1", state: transactions.VoteFailed},
				{name: "s2", state: transactions.VoteCommitted, err: anyErr},
			},
			replicas:                   []string{"r1", "r2"},
			didCommitAnySubtransaction: true,
			subtransactions:            1,
			expectedPrimaryDirtied:     true,
			expectedOutdated:           []string{"s1", "s2", "r1", "r2"},
			expectedMetrics: map[string]int{
				"node-failed":        1,
				"node-not-committed": 1,
				"outdated":           2,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			nodes := append(tc.secondaries, tc.primary)
			voters := make([]transactions.Voter, len(nodes))

			states := make(map[string]transactions.VoteResult)
			nodeErrors := &nodeErrors{
				errByNode: make(map[string]error),
			}

			for i, node := range nodes {
				voters[i] = transactions.Voter{
					Name:  node.name,
					Votes: 1,
				}
				states[node.name] = node.state
				nodeErrors.errByNode[node.name] = node.err
			}

			transaction := mockTransaction{
				nodeStates:                 states,
				subtransactions:            tc.subtransactions,
				didCommitAnySubtransaction: tc.didCommitAnySubtransaction,
			}

			route := RepositoryMutatorRoute{
				Primary: RouterNode{
					Storage: tc.primary.name,
				},
			}
			for _, secondary := range tc.secondaries {
				route.Secondaries = append(route.Secondaries, RouterNode{
					Storage: secondary.name,
				})
			}
			route.ReplicationTargets = append(route.ReplicationTargets, tc.replicas...)

			metric := prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "stub", Help: "help",
			}, []string{"reason"})

			primaryDirtied, updated, outdated := getUpdatedAndOutdatedSecondaries(ctx, route, transaction, nodeErrors, metric)
			require.Equal(t, tc.expectedPrimaryDirtied, primaryDirtied)
			require.ElementsMatch(t, tc.expectedUpdated, updated)
			require.ElementsMatch(t, tc.expectedOutdated, outdated)

			expectedMetrics := "# HELP stub help\n# TYPE stub counter\n"
			for metric, value := range tc.expectedMetrics {
				expectedMetrics += fmt.Sprintf("stub{reason=\"%s\"} %d\n", metric, value)
			}

			require.NoError(t, testutil.CollectAndCompare(metric, strings.NewReader(expectedMetrics)))
		})
	}
}

func TestNewRequestFinalizer_contextIsDisjointedFromTheRPC(t *testing.T) {
	type ctxKey struct{}

	parentDeadline := time.Now()
	ctx, cancel := context.WithDeadline(context.WithValue(context.Background(), ctxKey{}, "value"), parentDeadline)
	cancel()

	requireSuppressedCancellation := func(t testing.TB, ctx context.Context) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.NotEqual(t, parentDeadline, deadline)
		require.Equal(t, ctx.Value(ctxKey{}), "value")
		require.Nil(t, ctx.Err())
		select {
		case <-ctx.Done():
			t.Fatal("context should not be canceled if the parent is canceled")
		default:
			require.NotNil(t, ctx.Done())
		}
	}

	err := errors.New("error")

	for _, tc := range []struct {
		change datastore.ChangeType
		errMsg string
	}{
		{
			change: datastore.UpdateRepo,
			errMsg: "increment generation: error",
		},
		{
			change: datastore.RenameRepo,
			errMsg: "rename repository: error",
		},
		{
			change: datastore.DeleteRepo,
			errMsg: "delete repository: error",
		},
		{
			change: "replication jobs only",
			errMsg: "enqueue replication event: error",
		},
	} {
		t.Run(string(tc.change), func(t *testing.T) {
			require.EqualError(t,
				NewCoordinator(
					&datastore.MockReplicationEventQueue{
						EnqueueFunc: func(ctx context.Context, _ datastore.ReplicationEvent) (datastore.ReplicationEvent, error) {
							requireSuppressedCancellation(t, ctx)
							return datastore.ReplicationEvent{}, err
						},
					},
					datastore.MockRepositoryStore{
						IncrementGenerationFunc: func(ctx context.Context, _, _, _ string, _ []string) error {
							requireSuppressedCancellation(t, ctx)
							return err
						},
						RenameRepositoryFunc: func(ctx context.Context, _, _, _, _ string) error {
							requireSuppressedCancellation(t, ctx)
							return err
						},
						DeleteRepositoryFunc: func(ctx context.Context, _, _, _ string) error {
							requireSuppressedCancellation(t, ctx)
							return err
						},
						CreateRepositoryFunc: func(ctx context.Context, _, _, _ string, _, _ []string, _, _ bool) error {
							requireSuppressedCancellation(t, ctx)
							return err
						},
					},
					nil,
					nil,
					config.Config{},
					nil,
				).newRequestFinalizer(
					ctx,
					"virtual storage",
					&gitalypb.Repository{},
					"primary",
					[]string{},
					[]string{"secondary"},
					tc.change,
					datastore.Params{"RelativePath": "relative-path"},
					"rpc-name",
				)(),
				tc.errMsg,
			)
		})
	}
}
