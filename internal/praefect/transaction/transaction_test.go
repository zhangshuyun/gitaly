package transaction_test

import (
	"context"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/transaction"

	"github.com/stretchr/testify/require"
)

const (
	projA = "project-A"
	stor1 = "storage-1"
	stor2 = "storage-2"
	stor3 = "storage-3"
)

type testCase struct {
	name   string
	shards map[transaction.Repository]*transaction.Shard
	txList []struct {
		mutator bool
		repo    transaction.Repository
		txFn    func(transaction.Tx) error
	}
	expectErr error
}

var (
	repo1 = transaction.Repository{
		ProjectHash: projA,
		StorageLoc:  stor1,
	}

	testCases = []func(testing.TB) testCase{
		func(t testing.TB) testCase {
			var (
				node1 = &mockNode{
					tb:      t,
					storage: stor1,
					checksums: map[transaction.Repository][]string{
						repo1: []string{"1"},
					},
				}
			)

			return testCase{
				name: "one node shard: no-op access tx",
				shards: map[transaction.Repository]*transaction.Shard{
					repo1: transaction.NewShard(
						repo1,
						node1.storage,
						[]transaction.Node{node1},
					),
				},
				txList: []struct {
					mutator bool
					repo    transaction.Repository
					txFn    func(transaction.Tx) error
				}{
					{
						repo: repo1,
						txFn: func(_ transaction.Tx) error {
							t.Log("this is a no-op transaction")
							// checksum should get consumed during consistency
							// check
							require.Len(t, node1.checksums[repo1], 0)

							return nil
						},
					},
				},
			}
		},
	}
)

func TestManager(t *testing.T) {
	for _, ttFn := range testCases {
		tt := ttFn(t)
		t.Run(tt.name, func(t *testing.T) {
			var (
				mc  = mockCoordinator{tt.shards}
				mrm = mockReplMan{}
				rm  = transaction.NewManager(mc, mrm)
			)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			var err error
			for _, tx := range tt.txList {

				if tx.mutator {
					err = rm.Mutate(ctx, tx.repo, tx.txFn)
				} else {
					err = rm.Access(ctx, tx.repo, tx.txFn)
				}

				if err != nil {
					break
				}
			}

			if tt.expectErr != nil {
				require.Error(t, err)
				require.EqualError(t, err, tt.expectErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
