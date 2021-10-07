package praefect

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

func TestStreamDirectorMutator_Transaction(t *testing.T) {
	t.Parallel()

	type subtransactions []struct {
		vote          string
		shouldSucceed bool
	}

	type node struct {
		subtransactions    subtransactions
		shouldGetRepl      bool
		shouldParticipate  bool
		generation         int
		expectedGeneration int
	}

	testcases := []struct {
		desc                                 string
		primaryFails                         bool
		concurrentWrite                      bool
		nodes                                []node
		expectedRequestFinalizerErrorMessage string
	}{
		{
			desc: "successful vote should not create replication jobs",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
			},
		},
		{
			desc:         "successful vote should create replication jobs if the primary fails",
			primaryFails: true,
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
			},
		},
		{
			desc: "failing vote should create replication jobs without committed subtransactions",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foo", shouldSucceed: false}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "qux", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
				{subtransactions: subtransactions{{vote: "bar", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
			},
		},
		{
			desc: "failing vote should create replication jobs with committed subtransaction",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foo", shouldSucceed: true}, {vote: "foo", shouldSucceed: false}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "foo", shouldSucceed: true}, {vote: "qux", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
				{subtransactions: subtransactions{{vote: "foo", shouldSucceed: true}, {vote: "bar", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
			},
		},
		{
			desc: "primary should reach quorum with disagreeing secondary",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "barfoo", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
			},
		},
		{
			desc: "quorum should create replication jobs for disagreeing node",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldGetRepl: false, shouldParticipate: true, expectedGeneration: 1},
				{subtransactions: subtransactions{{vote: "barfoo", shouldSucceed: false}}, shouldGetRepl: true, shouldParticipate: true, expectedGeneration: 0},
			},
		},
		{
			desc: "only consistent secondaries should participate",
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldParticipate: true, generation: 1, expectedGeneration: 2},
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldParticipate: true, generation: 1, expectedGeneration: 2},
				{shouldParticipate: false, shouldGetRepl: true, generation: 0, expectedGeneration: 0},
				{shouldParticipate: false, shouldGetRepl: true, generation: datastore.GenerationUnknown, expectedGeneration: datastore.GenerationUnknown},
			},
		},
		{
			desc:            "write is not acknowledged if it only targets outdated nodes",
			concurrentWrite: true,
			nodes: []node{
				{subtransactions: subtransactions{{vote: "foobar", shouldSucceed: true}}, shouldParticipate: true, generation: 0, expectedGeneration: 0},
				{shouldParticipate: false, shouldGetRepl: false, generation: 0, expectedGeneration: 0},
			},
			expectedRequestFinalizerErrorMessage: "increment generation: write to outdated nodes",
		},
		{
			// All transactional RPCs are expected to cast vote if they are successful. If they don't, something is wrong
			// and we should replicate to the secondaries to be sure.
			desc: "unstarted transaction creates replication jobs if the primary is successful",
			nodes: []node{
				{shouldGetRepl: false, expectedGeneration: 1},
				{shouldGetRepl: true, expectedGeneration: 0},
			},
		},
		{
			desc:         "unstarted transaction does not create replication job",
			primaryFails: true,
			nodes: []node{
				{expectedGeneration: 0},
				{shouldGetRepl: false, expectedGeneration: 0},
			},
		},
		{
			desc:         "unstarted transaction should not create replication jobs for outdated node if the primary does not vote",
			primaryFails: true,
			nodes: []node{
				{shouldGetRepl: false, generation: 1, expectedGeneration: 1},
				{shouldGetRepl: false, generation: 1, expectedGeneration: 1},
				{shouldGetRepl: false, generation: 0, expectedGeneration: 0},
				{shouldGetRepl: false, generation: datastore.GenerationUnknown, expectedGeneration: datastore.GenerationUnknown},
			},
		},
		{
			// If there were no subtransactions and the primary did not fail, we should schedule replication jobs to every secondary.
			// All transactional RPCs are expected to vote if they are successful.
			desc: "unstarted transaction should create replication jobs for outdated node if the primary succeeds",
			nodes: []node{
				{shouldGetRepl: false, generation: 1, expectedGeneration: 2},
				{shouldGetRepl: true, generation: 1, expectedGeneration: 1},
				{shouldGetRepl: true, generation: 0, expectedGeneration: 0},
				{shouldGetRepl: true, generation: datastore.GenerationUnknown, expectedGeneration: datastore.GenerationUnknown},
			},
		},
	}

	db := glsql.NewDB(t)

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)

			storageNodes := make([]*config.Node, 0, len(tc.nodes))
			for i := range tc.nodes {
				socket := testhelper.GetTemporaryGitalySocketFileName(t)
				node := &config.Node{Address: "unix://" + socket, Storage: fmt.Sprintf("node-%d", i)}
				storageNodes = append(storageNodes, node)
			}

			conf := config.Config{
				VirtualStorages: []*config.VirtualStorage{
					{
						Name:  "praefect",
						Nodes: storageNodes,
					},
				},
			}

			var replicationWaitGroup sync.WaitGroup
			queueInterceptor := datastore.NewReplicationEventQueueInterceptor(datastore.NewPostgresReplicationEventQueue(db))
			queueInterceptor.OnEnqueue(func(ctx context.Context, event datastore.ReplicationEvent, queue datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
				defer replicationWaitGroup.Done()
				return queue.Enqueue(ctx, event)
			})

			virtualStorage := conf.VirtualStorages[0].Name
			repo := gitalypb.Repository{
				StorageName:  virtualStorage,
				RelativePath: "/path/to/hashed/repository",
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			txMgr := transactions.NewManager(conf)

			tx := db.Begin(t)
			defer tx.Rollback(t)

			// set up the generations prior to transaction
			rs := datastore.NewPostgresRepositoryStore(tx, conf.StorageNames())

			repoCreated := false
			for i, n := range tc.nodes {
				if n.generation == datastore.GenerationUnknown {
					continue
				}

				if !repoCreated {
					repoCreated = true
					require.NoError(t, rs.CreateRepository(ctx, 1, repo.StorageName, repo.RelativePath, storageNodes[i].Storage, nil, nil, true, false))
				}

				require.NoError(t, rs.SetGeneration(ctx, repo.StorageName, repo.RelativePath, storageNodes[i].Storage, n.generation))
			}

			testhelper.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect": conf.StorageNames()})

			nodeSet, err := DialNodes(
				ctx,
				conf.VirtualStorages,
				protoregistry.GitalyProtoPreregistered,
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)
			defer nodeSet.Close()

			coordinator := NewCoordinator(
				queueInterceptor,
				rs,
				NewPerRepositoryRouter(
					nodeSet.Connections(),
					nodes.NewPerRepositoryElector(tx),
					StaticHealthChecker(conf.StorageNames()),
					NewLockedRandom(rand.New(rand.NewSource(0))),
					rs,
					datastore.NewAssignmentStore(tx, conf.StorageNames()),
					rs,
					nil,
				),
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

			streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
			require.NoError(t, err)

			txCtx := peer.NewContext(streamParams.Primary().Ctx, &peer.Peer{})
			transaction, err := txinfo.TransactionFromContext(txCtx)
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

					for _, subtransaction := range node.subtransactions {
						vote := voting.VoteFromData([]byte(subtransaction.vote))
						err := txMgr.VoteTransaction(ctx, transaction.ID, fmt.Sprintf("node-%d", i), vote)
						if subtransaction.shouldSucceed {
							if !assert.NoError(t, err) {
								break
							}
						} else {
							if !assert.True(t, errors.Is(err, transactions.ErrTransactionFailed)) {
								break
							}
						}
					}
				}()
			}
			voterWaitGroup.Wait()

			if tc.primaryFails {
				require.Error(t, streamParams.Primary().ErrHandler(errors.New("rpc failure")))
			}

			if tc.concurrentWrite {
				require.NoError(t, rs.SetGeneration(ctx, repo.StorageName, repo.RelativePath, "non-participating-storage", 2))
			}

			err = streamParams.RequestFinalizer()
			if tc.expectedRequestFinalizerErrorMessage != "" {
				require.EqualError(t, err, tc.expectedRequestFinalizerErrorMessage)
			} else {
				require.NoError(t, err)
			}

			// Nodes that successfully committed should have their generations incremented.
			// Nodes that did not successfully commit or did not participate should remain on their
			// existing generation.
			for i, n := range tc.nodes {
				gen, err := rs.GetGeneration(ctx, repo.StorageName, repo.RelativePath, storageNodes[i].Storage)
				require.NoError(t, err)
				require.Equal(t, n.expectedGeneration, gen, "node %d has wrong generation", i)
			}

			tx.Commit(t)

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
