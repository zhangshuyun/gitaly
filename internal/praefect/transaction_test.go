package praefect

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type voter struct {
	votes         uint
	vote          string
	showsUp       bool
	shouldSucceed bool
}

func runPraefectServerAndTxMgr(t testing.TB) (*grpc.ClientConn, *transactions.Manager, testhelper.Cleanup) {
	conf := testConfig(1)
	txMgr := transactions.NewManager(conf)
	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{
		withTxMgr:   txMgr,
		withNodeMgr: nullNodeMgr{}, // to suppress node address issues
	})
	return cc, txMgr, cleanup
}

type counterMetrics struct {
	registered, started, invalid, committed, stopped int
}

func verifyCounterMetrics(t *testing.T, manager *transactions.Manager, expected counterMetrics) {
	t.Helper()

	metrics := []struct {
		name  string
		value int
	}{
		{"registered", expected.registered},
		{"started", expected.started},
		{"invalid", expected.invalid},
		{"committed", expected.committed},
		{"stopped", expected.stopped},
	}

	var expectedMetric bytes.Buffer
	expectedMetric.WriteString("# HELP gitaly_praefect_transactions_total Total number of transaction actions\n")
	expectedMetric.WriteString("# TYPE gitaly_praefect_transactions_total counter\n")
	for _, metric := range metrics {
		if metric.value == 0 {
			continue
		}
		expectedMetric.WriteString(fmt.Sprintf("gitaly_praefect_transactions_total{action=\"%s\"} %d\n", metric.name, metric.value))
	}

	require.NoError(t, testutil.CollectAndCompare(manager, &expectedMetric, "gitaly_praefect_transactions_total"))
}

func TestTransactionSucceeds(t *testing.T) {
	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
	defer cancel()

	client := gitalypb.NewRefTransactionClient(cc)

	transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, []transactions.Voter{
		{Name: "node1", Votes: 1},
	}, 1)
	require.NoError(t, err)
	require.NotNil(t, transaction)
	require.NotZero(t, transaction.ID())
	defer func() {
		require.NoError(t, cancelTransaction())
	}()

	hash := sha1.Sum([]byte{})

	response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        transaction.ID(),
		Node:                 "node1",
		ReferenceUpdatesHash: hash[:],
	})
	require.NoError(t, err)
	require.Equal(t, gitalypb.VoteTransactionResponse_COMMIT, response.State)

	verifyCounterMetrics(t, txMgr, counterMetrics{
		registered: 1,
		started:    1,
		committed:  1,
	})
}

func TestTransactionWithMultipleNodes(t *testing.T) {
	testcases := []struct {
		desc          string
		nodes         []string
		hashes        [][20]byte
		expectedState gitalypb.VoteTransactionResponse_TransactionState
	}{
		{
			desc: "Nodes with same hash",
			nodes: []string{
				"node1",
				"node2",
			},
			hashes: [][20]byte{
				sha1.Sum([]byte{}),
				sha1.Sum([]byte{}),
			},
			expectedState: gitalypb.VoteTransactionResponse_COMMIT,
		},
		{
			desc: "Nodes with different hashes",
			nodes: []string{
				"node1",
				"node2",
			},
			hashes: [][20]byte{
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("bar")),
			},
			expectedState: gitalypb.VoteTransactionResponse_ABORT,
		},
		{
			desc: "More nodes with same hash",
			nodes: []string{
				"node1",
				"node2",
				"node3",
				"node4",
			},
			hashes: [][20]byte{
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("foo")),
			},
			expectedState: gitalypb.VoteTransactionResponse_COMMIT,
		},
		{
			desc: "Majority with same hash",
			nodes: []string{
				"node1",
				"node2",
				"node3",
				"node4",
			},
			hashes: [][20]byte{
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("foo")),
				sha1.Sum([]byte("bar")),
				sha1.Sum([]byte("foo")),
			},
			expectedState: gitalypb.VoteTransactionResponse_ABORT,
		},
	}

	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	client := gitalypb.NewRefTransactionClient(cc)

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			var voters []transactions.Voter
			var threshold uint
			for _, node := range tc.nodes {
				voters = append(voters, transactions.Voter{Name: node, Votes: 1})
				threshold++
			}

			transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, threshold)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, cancelTransaction())
			}()

			var wg sync.WaitGroup
			for i := 0; i < len(voters); i++ {
				wg.Add(1)

				go func(idx int) {
					defer wg.Done()

					response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
						TransactionId:        transaction.ID(),
						Node:                 voters[idx].Name,
						ReferenceUpdatesHash: tc.hashes[idx][:],
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectedState, response.State)
				}(i)
			}

			wg.Wait()
		})
	}
}

func TestTransactionWithContextCancellation(t *testing.T) {
	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	client := gitalypb.NewRefTransactionClient(cc)

	ctx, cancel := testhelper.Context()

	transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, []transactions.Voter{
		{Name: "voter", Votes: 1},
		{Name: "absent", Votes: 1},
	}, 2)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, cancelTransaction())
	}()

	hash := sha1.Sum([]byte{})

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        transaction.ID(),
			Node:                 "voter",
			ReferenceUpdatesHash: hash[:],
		})
		require.Error(t, err)
		require.Equal(t, codes.Canceled, status.Code(err))
	}()

	cancel()
	wg.Wait()
}

func TestTransactionRegistrationWithInvalidNodesFails(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	txMgr := transactions.NewManager(config.Config{})

	_, _, err := txMgr.RegisterTransaction(ctx, []transactions.Voter{}, 1)
	require.Equal(t, transactions.ErrMissingNodes, err)

	_, _, err = txMgr.RegisterTransaction(ctx, []transactions.Voter{
		{Name: "node1", Votes: 1},
		{Name: "node2", Votes: 1},
		{Name: "node1", Votes: 1},
	}, 3)
	require.Equal(t, transactions.ErrDuplicateNodes, err)
}

func TestTransactionRegistrationWithInvalidThresholdFails(t *testing.T) {
	tc := []struct {
		desc      string
		votes     []uint
		threshold uint
	}{
		{
			desc:      "threshold is unreachable",
			votes:     []uint{1, 1},
			threshold: 3,
		},
		{
			desc:      "threshold of zero fails",
			votes:     []uint{0},
			threshold: 0,
		},
		{
			desc:      "threshold smaller than majority fails",
			votes:     []uint{1, 1, 1},
			threshold: 1,
		},
		{
			desc:      "threshold equaling majority fails",
			votes:     []uint{1, 1, 1, 1},
			threshold: 2,
		},
		{
			desc:      "threshold accounts for higher node votes",
			votes:     []uint{2, 2, 2, 2},
			threshold: 4,
		},
	}

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	txMgr := transactions.NewManager(config.Config{})

	for _, tc := range tc {
		t.Run(tc.desc, func(t *testing.T) {
			var voters []transactions.Voter

			for i, votes := range tc.votes {
				voters = append(voters, transactions.Voter{
					Name:  fmt.Sprintf("node-%d", i),
					Votes: votes,
				})
			}

			_, _, err := txMgr.RegisterTransaction(ctx, voters, tc.threshold)
			require.Equal(t, transactions.ErrInvalidThreshold, err)
		})
	}
}

func TestTransactionReachesQuorum(t *testing.T) {
	tc := []struct {
		desc      string
		voters    []voter
		threshold uint
	}{
		{
			desc: "quorum is is not reached without majority",
			voters: []voter{
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: false},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: false},
				{votes: 1, vote: "baz", showsUp: true, shouldSucceed: false},
			},
			threshold: 2,
		},
		{
			desc: "quorum is reached with unweighted node failing",
			voters: []voter{
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 0, vote: "bar", showsUp: true, shouldSucceed: false},
			},
			threshold: 1,
		},
		{
			desc: "quorum is reached with majority",
			voters: []voter{
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: false},
			},
			threshold: 2,
		},
		{
			desc: "quorum is reached with high vote outweighing",
			voters: []voter{
				{votes: 3, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: false},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: false},
			},
			threshold: 3,
		},
		{
			desc: "quorum is reached with high vote being outweighed",
			voters: []voter{
				{votes: 3, vote: "foo", showsUp: true, shouldSucceed: false},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: true, shouldSucceed: true},
			},
			threshold: 4,
		},
		{
			desc: "quorum is reached with disappearing unweighted voter",
			voters: []voter{
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 0, vote: "foo", showsUp: false, shouldSucceed: false},
			},
			threshold: 1,
		},
		{
			desc: "quorum is reached with disappearing weighted voter",
			voters: []voter{
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "foo", showsUp: true, shouldSucceed: true},
				{votes: 1, vote: "bar", showsUp: false, shouldSucceed: false},
			},
			threshold: 2,
		},
	}

	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	client := gitalypb.NewRefTransactionClient(cc)

	for _, tc := range tc {
		t.Run(tc.desc, func(t *testing.T) {
			var voters []transactions.Voter

			for i, voter := range tc.voters {
				voters = append(voters, transactions.Voter{
					Name:  fmt.Sprintf("node-%d", i),
					Votes: voter.votes,
				})
			}

			transaction, cancel, err := txMgr.RegisterTransaction(ctx, voters, tc.threshold)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, cancel())
			}()

			var wg sync.WaitGroup
			for i, v := range tc.voters {
				if !v.showsUp {
					continue
				}

				wg.Add(1)
				go func(i int, v voter) {
					defer wg.Done()

					name := fmt.Sprintf("node-%d", i)
					hash := sha1.Sum([]byte(v.vote))

					response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
						TransactionId:        transaction.ID(),
						Node:                 name,
						ReferenceUpdatesHash: hash[:],
					})
					require.NoError(t, err)

					if v.shouldSucceed {
						require.Equal(t, gitalypb.VoteTransactionResponse_COMMIT, response.State, "node should have received COMMIT")
					} else {
						require.Equal(t, gitalypb.VoteTransactionResponse_ABORT, response.State, "node should have received ABORT")
					}
				}(i, v)
			}

			wg.Wait()
		})
	}
}

func TestTransactionWithMultipleVotes(t *testing.T) {
	type multiVoter struct {
		voteCount      uint
		votes          []string
		voteSucceeds   []bool
		expectedResult transactions.VoteResult
	}

	tc := []struct {
		desc      string
		voters    []multiVoter
		threshold uint
	}{
		{
			desc: "quorum is reached with multiple votes",
			voters: []multiVoter{
				{voteCount: 1, votes: []string{"foo", "bar"}, voteSucceeds: []bool{true, true}, expectedResult: transactions.VoteCommitted},
				{voteCount: 1, votes: []string{"foo", "bar"}, voteSucceeds: []bool{true, true}, expectedResult: transactions.VoteCommitted},
			},
			threshold: 2,
		},
		{
			desc: "quorum is not reached with disagreeing votes",
			voters: []multiVoter{
				{voteCount: 1, votes: []string{"foo", "bar"}, voteSucceeds: []bool{true, false}, expectedResult: transactions.VoteFailed},
				{voteCount: 1, votes: []string{"foo", "rab"}, voteSucceeds: []bool{true, false}, expectedResult: transactions.VoteFailed},
			},
			threshold: 2,
		},
		{
			desc: "quorum is reached with unweighted disagreeing voter",
			voters: []multiVoter{
				{voteCount: 1, votes: []string{"foo", "bar", "qux"}, voteSucceeds: []bool{true, true, true}, expectedResult: transactions.VoteCommitted},
				{voteCount: 0, votes: []string{"foo", "rab"}, voteSucceeds: []bool{true, false}, expectedResult: transactions.VoteCanceled},
			},
			threshold: 1,
		},
		{
			desc: "quorum is reached with outweighed disagreeing voter",
			voters: []multiVoter{
				{voteCount: 1, votes: []string{"foo", "bar", "qux"}, voteSucceeds: []bool{true, true, true}, expectedResult: transactions.VoteCommitted},
				{voteCount: 1, votes: []string{"foo", "bar", "qux"}, voteSucceeds: []bool{true, true, true}, expectedResult: transactions.VoteCommitted},
				{voteCount: 1, votes: []string{"foo", "rab"}, voteSucceeds: []bool{true, false}, expectedResult: transactions.VoteCanceled},
			},
			threshold: 2,
		},
	}

	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	client := gitalypb.NewRefTransactionClient(cc)

	for _, tc := range tc {
		t.Run(tc.desc, func(t *testing.T) {
			var voters []transactions.Voter

			for i, voter := range tc.voters {
				voters = append(voters, transactions.Voter{
					Name:  fmt.Sprintf("node-%d", i),
					Votes: voter.voteCount,
				})
			}

			transaction, cancel, err := txMgr.RegisterTransaction(ctx, voters, tc.threshold)
			require.NoError(t, err)

			var wg sync.WaitGroup
			for i, v := range tc.voters {
				wg.Add(1)
				go func(i int, v multiVoter) {
					defer wg.Done()

					for j, vote := range v.votes {
						name := fmt.Sprintf("node-%d", i)
						hash := sha1.Sum([]byte(vote))

						response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
							TransactionId:        transaction.ID(),
							Node:                 name,
							ReferenceUpdatesHash: hash[:],
						})
						assert.NoError(t, err)

						if v.voteSucceeds[j] {
							assert.Equal(t, gitalypb.VoteTransactionResponse_COMMIT, response.State, "node should have received COMMIT")
						} else {
							assert.Equal(t, gitalypb.VoteTransactionResponse_ABORT, response.State, "node should have received ABORT")
						}
					}
				}(i, v)
			}

			wg.Wait()

			require.NoError(t, cancel())
			results, err := transaction.State()
			require.NoError(t, err)
			for i, voter := range tc.voters {
				require.Equal(t, voter.expectedResult, results[fmt.Sprintf("node-%d", i)])
			}
		})
	}
}

func TestTransactionFailures(t *testing.T) {
	cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
	defer cleanup()

	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
	defer cancel()

	client := gitalypb.NewRefTransactionClient(cc)

	hash := sha1.Sum([]byte{})
	_, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        1,
		Node:                 "node1",
		ReferenceUpdatesHash: hash[:],
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))

	verifyCounterMetrics(t, txMgr, counterMetrics{
		started: 1,
		invalid: 1,
	})
}

func TestTransactionCancellation(t *testing.T) {
	testcases := []struct {
		desc            string
		voters          []voter
		threshold       uint
		expectedMetrics counterMetrics
	}{
		{
			desc: "single node cancellation",
			voters: []voter{
				{votes: 1, showsUp: false, shouldSucceed: false},
			},
			threshold:       1,
			expectedMetrics: counterMetrics{registered: 1, committed: 0},
		},
		{
			desc: "two nodes failing to show up",
			voters: []voter{
				{votes: 1, showsUp: false, shouldSucceed: false},
				{votes: 1, showsUp: false, shouldSucceed: false},
			},
			threshold:       2,
			expectedMetrics: counterMetrics{registered: 2, committed: 0},
		},
		{
			desc: "two nodes with unweighted node failing",
			voters: []voter{
				{votes: 1, showsUp: true, shouldSucceed: true},
				{votes: 0, showsUp: false, shouldSucceed: false},
			},
			threshold:       1,
			expectedMetrics: counterMetrics{registered: 2, started: 1, committed: 1},
		},
		{
			desc: "multiple weighted votes with subset failing",
			voters: []voter{
				{votes: 1, showsUp: true, shouldSucceed: true},
				{votes: 1, showsUp: true, shouldSucceed: true},
				{votes: 1, showsUp: false, shouldSucceed: false},
			},
			threshold:       2,
			expectedMetrics: counterMetrics{registered: 3, started: 2, committed: 2},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
			defer cleanup()

			ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
			defer cancel()

			client := gitalypb.NewRefTransactionClient(cc)

			voters := make([]transactions.Voter, 0, len(tc.voters))
			for i, voter := range tc.voters {
				voters = append(voters, transactions.Voter{
					Name:  fmt.Sprintf("node-%d", i),
					Votes: voter.votes,
				})
			}

			transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, tc.threshold)
			require.NoError(t, err)

			var wg sync.WaitGroup
			for i, v := range tc.voters {
				if !v.showsUp {
					continue
				}

				wg.Add(1)
				go func(i int, v voter) {
					defer wg.Done()

					name := fmt.Sprintf("node-%d", i)
					hash := sha1.Sum([]byte(v.vote))

					response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
						TransactionId:        transaction.ID(),
						Node:                 name,
						ReferenceUpdatesHash: hash[:],
					})
					require.NoError(t, err)

					if v.shouldSucceed {
						require.Equal(t, gitalypb.VoteTransactionResponse_COMMIT, response.State, "node should have received COMMIT")
					} else {
						require.Equal(t, gitalypb.VoteTransactionResponse_ABORT, response.State, "node should have received ABORT")
					}
				}(i, v)
			}
			wg.Wait()

			require.NoError(t, cancelTransaction())

			results, err := transaction.State()
			require.NoError(t, err)
			for i, v := range tc.voters {
				if v.shouldSucceed {
					require.Equal(t, transactions.VoteCommitted, results[fmt.Sprintf("node-%d", i)], "result mismatches expected node state")
				} else if v.showsUp {
					require.Equal(t, transactions.VoteFailed, results[fmt.Sprintf("node-%d", i)], "result mismatches expected node state")
				} else {
					require.Equal(t, transactions.VoteCanceled, results[fmt.Sprintf("node-%d", i)], "result mismatches expected node state")
				}
			}

			verifyCounterMetrics(t, txMgr, tc.expectedMetrics)
		})
	}
}

func TestStopTransaction(t *testing.T) {
	hash := sha1.Sum([]byte("foo"))

	t.Run("stopping nonexisting transaction fails", func(t *testing.T) {
		cc, _, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		_, err := client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
			TransactionId: 1234,
		})
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("stopping transaction multiple times succeeds", func(t *testing.T) {
		cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		voters := []transactions.Voter{
			{Name: "successful-voter", Votes: 2},
			{Name: "failing-voter", Votes: 1},
		}

		transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, 2)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cancelTransaction())
		}()

		for i := 0; i < 5; i++ {
			_, err = client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
				TransactionId: transaction.ID(),
			})
			require.NoError(t, err)
		}

		verifyCounterMetrics(t, txMgr, counterMetrics{
			registered: 2,
			stopped:    5,
		})
	})

	t.Run("stopping a single voter", func(t *testing.T) {
		cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		voters := []transactions.Voter{
			{Name: "voter", Votes: 1},
		}

		transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, 1)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cancelTransaction())
		}()

		_, err = client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
			TransactionId: transaction.ID(),
		})
		require.NoError(t, err)

		response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        transaction.ID(),
			Node:                 "voter",
			ReferenceUpdatesHash: hash[:],
		})
		require.NoError(t, err)
		require.Equal(t, gitalypb.VoteTransactionResponse_STOP, response.State)

		results, err := transaction.State()
		require.NoError(t, err)
		require.Equal(t, transactions.VoteStopped, results["voter"])
		verifyCounterMetrics(t, txMgr, counterMetrics{
			registered: 1,
			started:    1,
			stopped:    2,
		})
	})

	t.Run("stopping in-progress transaction", func(t *testing.T) {
		cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		voters := []transactions.Voter{
			{Name: "successful-voter", Votes: 2},
			{Name: "failing-voter", Votes: 1},
		}

		transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, 2)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cancelTransaction())
		}()

		response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        transaction.ID(),
			Node:                 "successful-voter",
			ReferenceUpdatesHash: hash[:],
		})
		require.NoError(t, err)
		require.Equal(t, gitalypb.VoteTransactionResponse_COMMIT, response.State)

		_, err = client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
			TransactionId: transaction.ID(),
		})
		require.NoError(t, err)

		response, err = client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
			TransactionId:        transaction.ID(),
			Node:                 "failing-voter",
			ReferenceUpdatesHash: hash[:],
		})
		require.NoError(t, err)
		require.Equal(t, gitalypb.VoteTransactionResponse_STOP, response.State)

		results, err := transaction.State()
		require.NoError(t, err)
		require.Equal(t, transactions.VoteCommitted, results["successful-voter"], "Successful voter should succeed")
		require.Equal(t, transactions.VoteStopped, results["failing-voter"], "Failing voter should fail")
		verifyCounterMetrics(t, txMgr, counterMetrics{
			committed:  1,
			registered: 2,
			started:    2,
			stopped:    2,
		})
	})

	t.Run("stopping cancelled transaction fails", func(t *testing.T) {
		cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		voters := []transactions.Voter{
			{Name: "successful-voter", Votes: 2},
			{Name: "failing-voter", Votes: 1},
		}

		transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, 2)
		require.NoError(t, err)

		require.NoError(t, cancelTransaction())

		_, err = client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
			TransactionId: transaction.ID(),
		})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("stopping concurrent voter", func(t *testing.T) {
		cc, txMgr, cleanup := runPraefectServerAndTxMgr(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		client := gitalypb.NewRefTransactionClient(cc)

		voters := []transactions.Voter{
			{Name: "1", Votes: 1},
			{Name: "2", Votes: 1},
		}

		transaction, cancelTransaction, err := txMgr.RegisterTransaction(ctx, voters, 2)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cancelTransaction())
		}()

		// This create a single voter waiting for the threshold to be
		// reached. As the second voter will never appear, the node
		// will instead be stopped by the call to `StopTransaction`
		// below.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			hash := sha1.Sum([]byte("hash"))
			response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
				TransactionId:        transaction.ID(),
				Node:                 "1",
				ReferenceUpdatesHash: hash[:],
			})
			require.NoError(t, err)
			require.Equal(t, gitalypb.VoteTransactionResponse_STOP, response.State)
		}()

		_, err = client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
			TransactionId: transaction.ID(),
		})
		require.NoError(t, err)

		wg.Wait()
	})
}
