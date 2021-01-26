// +build postgres

package praefect

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	praefect_metadata "gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func getDB(t *testing.T) glsql.DB {
	return glsql.GetDB(t, "praefect")
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
		desc  string
		nodes []node
	}{
		{
			desc: "successful vote should not create replication jobs",
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
			},
		},
		{
			desc: "failing vote should not create replication jobs",
			nodes: []node{
				{primary: true, vote: "foo", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "qux", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "bar", shouldSucceed: false, shouldGetRepl: false, shouldParticipate: true},
			},
		},
		{
			desc: "primary should reach quorum with disagreeing secondary",
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "barfoo", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
			},
		},
		{
			desc: "quorum should create replication jobs for disagreeing node",
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldGetRepl: false, shouldParticipate: true},
				{primary: false, vote: "barfoo", shouldSucceed: false, shouldGetRepl: true, shouldParticipate: true},
			},
		},
		{
			desc: "only consistent secondaries should participate",
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: 1},
				{primary: false, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: 1},
				{shouldParticipate: false, shouldGetRepl: true, generation: 0},
				{shouldParticipate: false, shouldGetRepl: true, generation: datastore.GenerationUnknown},
			},
		},
		{
			desc: "secondaries should not participate when primary's generation is unknown",
			nodes: []node{
				{primary: true, vote: "foobar", shouldSucceed: true, shouldParticipate: true, generation: datastore.GenerationUnknown},
				{shouldParticipate: false, shouldGetRepl: true, generation: datastore.GenerationUnknown},
			},
		},
		{
			// If the transaction didn't receive any votes at all, we need to assume
			// that the RPC wasn't aware of transactions and thus need to schedule
			// replication jobs.
			desc: "unstarted transaction should create replication jobs",
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
				socket := testhelper.GetTemporaryGitalySocketFileName(t)
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
				RelativePath: "/path/to/hashed/repository",
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			nodeMgr, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil)
			require.NoError(t, err)
			nodeMgr.Start(0, time.Hour)

			shard, err := nodeMgr.GetShard(ctx, conf.VirtualStorages[0].Name)
			require.NoError(t, err)

			for i := range tc.nodes {
				node, err := shard.GetNode(fmt.Sprintf("node-%d", i))
				require.NoError(t, err)
				waitNodeToChangeHealthStatus(ctx, t, node, true)
			}

			txMgr := transactions.NewManager(conf)

			// set up the generations prior to transaction
			rs := datastore.NewPostgresRepositoryStore(getDB(t), conf.StorageNames())
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

			streamParams, err := coordinator.StreamDirector(ctx, fullMethod, peeker)
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
