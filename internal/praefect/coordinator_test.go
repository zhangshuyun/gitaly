package praefect

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

var testLogger = logrus.New()

func init() {
	testLogger.SetOutput(ioutil.Discard)
}

func TestSecondaryRotation(t *testing.T) {
	t.Skip("secondary rotation will change with the new data model")
}

type mockNodeManager struct {
	GetShardFunc func(string) (nodes.Shard, error)
}

func (m *mockNodeManager) GetShard(storage string) (nodes.Shard, error) {
	return m.GetShardFunc(storage)
}

func (m *mockNodeManager) EnableWrites(context.Context, string) error { panic("unimplemented") }

func (m *mockNodeManager) GetSyncedNode(context.Context, string, string) (nodes.Node, error) {
	panic("unimplemented")
}

type mockNode struct {
	nodes.Node
	storageName string
	conn        *grpc.ClientConn
}

func (m *mockNode) GetStorage() string { return m.storageName }

func (m *mockNode) GetConnection() *grpc.ClientConn { return m.conn }

func (m *mockNode) GetAddress() string { return "" }

func (m *mockNode) GetToken() string { return "" }

func TestStreamDirectorReadOnlyEnforcement(t *testing.T) {
	for _, tc := range []struct {
		readOnly        bool
		readOnlyEnabled bool
		shouldError     bool
	}{
		{
			readOnly:        false,
			readOnlyEnabled: true,
			shouldError:     false,
		},
		{
			readOnly:        true,
			readOnlyEnabled: true,
			shouldError:     true,
		},
		{
			readOnly:        false,
			readOnlyEnabled: false,
			shouldError:     false,
		},
		{
			readOnly:        true,
			readOnlyEnabled: false,
			shouldError:     false,
		},
	} {
		t.Run(fmt.Sprintf("read-only: %v, enabled: %v", tc.readOnly, tc.readOnlyEnabled), func(t *testing.T) {
			conf := config.Config{
				Failover: config.Failover{ReadOnlyAfterFailover: tc.readOnlyEnabled},
				VirtualStorages: []*config.VirtualStorage{
					&config.VirtualStorage{
						Name: "praefect",
						Nodes: []*config.Node{
							&config.Node{
								Address:        "tcp://gitaly-primary.example.com",
								Storage:        "praefect-internal-1",
								DefaultPrimary: true,
							},
						},
					},
				},
			}

			const storageName = "test-storage"
			coordinator := NewCoordinator(
				datastore.NewMemoryReplicationEventQueue(conf),
				&mockNodeManager{GetShardFunc: func(storage string) (nodes.Shard, error) {
					return nodes.Shard{
						IsReadOnly: tc.readOnly,
						Primary:    &mockNode{storageName: storageName},
					}, nil
				}},
				transactions.NewManager(),
				conf,
				protoregistry.GitalyProtoPreregistered,
			)

			ctx, cancel := testhelper.Context()
			defer cancel()

			frame, err := proto.Marshal(&gitalypb.CleanupRequest{Repository: &gitalypb.Repository{
				StorageName:  storageName,
				RelativePath: "only-for-validation",
			}})
			require.NoError(t, err)

			_, err = coordinator.StreamDirector(ctx, "/gitaly.RepositoryService/Cleanup", &mockPeeker{frame: frame})
			if tc.shouldError {
				require.True(t, errors.Is(err, ReadOnlyStorageError(storageName)))
				testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestStreamDirectorMutator(t *testing.T) {
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, _ := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryAddress, secondaryAddress := "unix://"+gitalySocket0, "unix://"+gitalySocket1
	primaryNode := &config.Node{Address: primaryAddress, Storage: "praefect-internal-1", DefaultPrimary: true}
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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, queueInterceptor, promtest.NewMockHistogramVec())
	require.NoError(t, err)

	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(queueInterceptor, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"

	peeker := &mockPeeker{frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, primaryAddress, streamParams.Primary().Conn.Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
	require.True(t, ok)
	require.Contains(t, md, "praefect-server")

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

func TestStreamDirectorAccessor(t *testing.T) {
	gitalySocket := testhelper.GetTemporaryGitalySocketFileName()
	srv, _ := testhelper.NewServerWithHealth(t, gitalySocket)
	defer srv.Stop()

	gitalyAddress := "unix://" + gitalySocket
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Address:        gitalyAddress,
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, queue, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	nodeMgr.Start(0, time.Minute)

	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(queue, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
	require.NoError(t, err)

	fullMethod := "/gitaly.RefService/FindAllBranches"

	peeker := &mockPeeker{frame: frame}
	streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
	require.NoError(t, err)
	require.Equal(t, gitalyAddress, streamParams.Primary().Conn.Target())

	md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
	require.True(t, ok)
	require.Contains(t, md, "praefect-server")

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
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
	srv1, _ := testhelper.NewServerWithHealth(t, gitalySocket0)
	defer srv1.Stop()
	srv2, healthSrv := testhelper.NewServerWithHealth(t, gitalySocket1)
	defer srv2.Stop()

	primaryNodeConf := config.Node{
		Address:        "unix://" + gitalySocket0,
		Storage:        "gitaly-1",
		DefaultPrimary: true,
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
	ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, featureflag.DistributedReads)

	entry := testhelper.DiscardTestEntry(t)

	nodeMgr, err := nodes.NewManager(entry, conf, nil, queue, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	nodeMgr.Start(0, time.Minute)

	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(queue, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	t.Run("forwards accessor operations", func(t *testing.T) {
		frame, err := proto.Marshal(&gitalypb.FindAllBranchesRequest{Repository: &targetRepo})
		require.NoError(t, err)

		fullMethod := "/gitaly.RefService/FindAllBranches"

		peeker := &mockPeeker{frame: frame}
		streamParams, err := coordinator.StreamDirector(correlation.ContextWithCorrelation(ctx, "my-correlation-id"), fullMethod, peeker)
		require.NoError(t, err)
		require.Equal(t, secondaryNodeConf.Address, streamParams.Primary().Conn.Target(), "must be redirected to secondary")

		md, ok := metadata.FromOutgoingContext(streamParams.Primary().Ctx)
		require.True(t, ok)
		require.Contains(t, md, "praefect-server")

		mi, err := coordinator.registry.LookupMethod(fullMethod)
		require.NoError(t, err)
		require.Equal(t, protoregistry.OpAccessor, mi.Operation, "method must be an accessor")

		m, err := protoMessage(mi, streamParams.Primary().Msg)
		require.NoError(t, err)

		rewrittenTargetRepo, err := mi.TargetRepo(m)
		require.NoError(t, err)
		require.Equal(t, "gitaly-2", rewrittenTargetRepo.GetStorageName(), "stream director must rewrite the storage name")
	})

	t.Run("forwards accessor operations only to healthy nodes", func(t *testing.T) {
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		shard, err := nodeMgr.GetShard(conf.VirtualStorages[0].Name)
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
		require.Contains(t, md, "praefect-server")

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
		require.Contains(t, md, "praefect-server")

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
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
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
						Address:        primaryAddress,
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					&config.Node{
						Address: secondaryAddress,
						Storage: "praefect-internal-2",
					}},
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

	nodeMgr, err := nodes.NewManager(entry, conf, nil, queueInterceptor, promtest.NewMockHistogramVec())
	require.NoError(t, err)
	txMgr := transactions.NewManager()

	coordinator := NewCoordinator(queueInterceptor, nodeMgr, txMgr, conf, protoregistry.GitalyProtoPreregistered)

	frame, err := proto.Marshal(&gitalypb.FetchIntoObjectPoolRequest{
		Origin:     &targetRepo,
		ObjectPool: &gitalypb.ObjectPool{Repository: &targetRepo},
		Repack:     false,
	})
	require.NoError(t, err)

	fullMethod := "/gitaly.ObjectPoolService/FetchIntoObjectPool"
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
