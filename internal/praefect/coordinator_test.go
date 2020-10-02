package praefect

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	praefect_metadata "gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
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

			rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())
			require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, "latest", 1))
			require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, 1))
			if tc.readOnly {
				require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, storage, 0))
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
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
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
	rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())

	coordinator := NewCoordinator(
		queueInterceptor,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

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

func TestStreamDirectorMutator_Transaction(t *testing.T) {
	type node struct {
		primary           bool
		vote              string
		shouldSucceed     bool
		shouldGetRepl     bool
		shouldParticipate bool
		generation        int
	}

	testcases := []struct {
		desc            string
		disableFeatures []featureflag.FeatureFlag
		nodes           []node
	}{
		{
			desc:            "successful vote should not create replication jobs",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
			},
		},
		{
			desc:            "failing vote should not create replication jobs",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foo", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "qux", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "bar", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
			},
		},
		{
			desc:            "primary should reach quorum with disagreeing secondary",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "barfoo", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
			},
		},
		{
			desc:            "quorum should create replication jobs for disagreeing node",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "barfoo", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
			},
		},
		{
			desc:            "only consistent secondaries should participate",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: 1},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: 1},
				{shouldParticipate: false, generation: 0},
				{shouldParticipate: false, generation: datastore.GenerationUnknown},
			},
		},
		{
			desc:            "secondaries should not participate when primary's generation is unknown",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: datastore.GenerationUnknown},
				{shouldParticipate: false, generation: datastore.GenerationUnknown},
			},
		},
		{
			desc: "primary succeeds with primary-wins strategy",
			nodes: []node{
				{primary: true, vote: "foo", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "qux", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
				{primary: false, vote: "bar", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
			},
		},
		{
			desc: "only failing secondaries get replication jobs",
			nodes: []node{
				{primary: true, vote: "foo", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "qux", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
				{primary: false, vote: "foo", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
			},
		},
		{
			// If the transaction didn't receive any votes at all, we need to assume
			// that the RPC wasn't aware of transactions and thus need to schedule
			// replication jobs.
			desc:            "unstarted transaction should create replication jobs",
			disableFeatures: []featureflag.FeatureFlag{featureflag.ReferenceTransactionsPrimaryWins},
			nodes: []node{
				{primary: true, shouldSucceed: true, shouldGetRepl: false},
				{primary: false, shouldSucceed: false, shouldGetRepl: true},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			storageNodes := make([]*config.Node, 0, len(tc.nodes))
			for i := range tc.nodes {
				socket := testhelper.GetTemporaryGitalySocketFileName()
				server, _ := testhelper.NewServerWithHealth(t, socket)
				defer server.Stop()
				node := &config.Node{Address: "unix://" + socket, Storage: fmt.Sprintf("node-%d", i)}
				storageNodes = append(storageNodes, node)
			}

			conf := config.Config{
				VirtualStorages: []*config.VirtualStorage{
					&config.VirtualStorage{
						Name:  "praefect",
						Nodes: storageNodes,
					},
				},
			}

			var replicationWaitGroup sync.WaitGroup
			queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
			queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
				defer replicationWaitGroup.Done()
				return queue.Enqueue(ctx, event)
			})

			repo := gitalypb.Repository{
				StorageName:  "praefect",
				RelativePath: "/path/to/hashed/storage",
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			for _, feature := range tc.disableFeatures {
				ctx = featureflag.IncomingCtxWithDisabledFeatureFlag(ctx, feature)
			}

			nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
			require.NoError(t, err)
			nodeMgr.Start(0, time.Hour)

			shard, err := nodeMgr.GetShard(conf.VirtualStorages[0].Name)
			require.NoError(t, err)

			for i := range tc.nodes {
				node, err := shard.GetNode(fmt.Sprintf("node-%d", i))
				require.NoError(t, err)
				waitNodeToChangeHealthStatus(ctx, t, node, true)
			}

			txMgr := transactions.NewManager(conf)

			// set up the generations prior to transaction
			rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())
			for i, n := range tc.nodes {
				if n.generation == datastore.GenerationUnknown {
					continue
				}

				require.NoError(t, rs.SetGeneration(ctx, repo.StorageName, repo.RelativePath, storageNodes[i].Storage, n.generation))
			}

			coordinator := NewCoordinator(
				queueInterceptor,
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

			var voterWaitGroup sync.WaitGroup
			for i, node := range tc.nodes {
				if node.shouldGetRepl {
					replicationWaitGroup.Add(1)
				}

				if !node.shouldParticipate {
					continue
				}

				i := i
				node := node

				voterWaitGroup.Add(1)
				go func() {
					defer voterWaitGroup.Done()

					vote := sha1.Sum([]byte(node.vote))
					err := txMgr.VoteTransaction(ctx, transaction.ID, fmt.Sprintf("node-%d", i), vote[:])
					if node.shouldSucceed {
						assert.NoError(t, err)
					} else {
						assert.True(t, errors.Is(err, transactions.ErrTransactionVoteFailed))
					}
				}()
			}
			voterWaitGroup.Wait()

			// this call creates new events in the queue and simulates usual flow of the update operation
			var primaryShouldSucceed bool
			for _, node := range tc.nodes {
				if !node.primary {
					continue
				}
				primaryShouldSucceed = node.shouldSucceed
			}
			err = streamParams.RequestFinalizer()
			if primaryShouldSucceed {
				require.NoError(t, err)
			} else {
				require.Equal(t, errors.New("transaction: primary failed vote"), err)
			}

			// Nodes that successfully committed should have their generations incremented.
			// Nodes that did not successfully commit or did not participate should remain on their
			// existing generation.
			for i, n := range tc.nodes {
				gen, err := rs.GetGeneration(ctx, repo.StorageName, repo.RelativePath, storageNodes[i].Storage)
				require.NoError(t, err)
				expectedGeneration := n.generation
				if n.shouldSucceed {
					expectedGeneration++
				}
				require.Equal(t, expectedGeneration, gen)
			}

			replicationWaitGroup.Wait()

			for i, node := range tc.nodes {
				events, err := queueInterceptor.Dequeue(ctx, "praefect", fmt.Sprintf("node-%d", i), 10)
				require.NoError(t, err)
				if node.shouldGetRepl {
					require.Len(t, events, 1)
				} else {
					require.Empty(t, events)
				}
			}
		})
	}
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
	rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())

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
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
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

	repoStore := datastore.NewMemoryRepositoryStore(conf.StorageNames())
	require.NoError(t, repoStore.IncrementGeneration(ctx, conf.VirtualStorages[0].Name, targetRepo.RelativePath, primaryNodeConf.Storage, nil))
	generation, err := repoStore.GetGeneration(ctx, conf.VirtualStorages[0].Name, targetRepo.RelativePath, primaryNodeConf.Storage)
	require.NoError(t, err)
	require.NoError(t, repoStore.SetGeneration(ctx, conf.VirtualStorages[0].Name, targetRepo.RelativePath, secondaryNodeConf.Storage, generation))

	nodeMgr, err := nodes.NewManager(entry, conf, nil, repoStore, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
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
	rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())

	coordinator := NewCoordinator(
		queueInterceptor,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		protoregistry.GitalyProtoPreregistered,
	)

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
	gitalySocket0, gitalySocket1 := testhelper.GetTemporaryGitalySocketFileName(), testhelper.GetTemporaryGitalySocketFileName()
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

	rs := datastore.NewMemoryRepositoryStore(conf.StorageNames())

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

		rs := datastore.NewMemoryRepositoryStore(nil)
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

		rs := datastore.NewMemoryRepositoryStore(nil)
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

			rs := datastore.NewMemoryRepositoryStore(nil)
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
			rs := datastore.NewMemoryRepositoryStore(nil)
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

func requireScopeOperation(t *testing.T, registry *protoregistry.Registry, fullMethod string, scope protoregistry.Scope, op protoregistry.OpType) {
	t.Helper()

	mi, err := registry.LookupMethod(fullMethod)
	require.NoError(t, err)
	require.Equal(t, scope, mi.Scope, "scope doesn't match requested")
	require.Equal(t, op, mi.Operation, "operation type doesn't match requested")
}
