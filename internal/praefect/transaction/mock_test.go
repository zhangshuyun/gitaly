package transaction_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/transaction"
)

type mockCoordinator struct {
	shards map[transaction.Repository]*transaction.Shard
}

func (mc mockCoordinator) FetchShard(_ context.Context, repo transaction.Repository) (*transaction.Shard, error) {
	s, ok := mc.shards[repo]
	if !ok {
		return nil, fmt.Errorf("shard doesn't exist for repo %+v", repo)
	}

	return s, nil
}

type mockReplMan struct{}

func (_ mockReplMan) NotifyDegradation(context.Context, transaction.Repository) error { return nil }

type mockNode struct {
	sync.RWMutex

	tb  testing.TB
	rpc *transaction.RPC

	// set the following values in test cases
	storage   string
	checksums map[transaction.Repository][]string
}

func (mn *mockNode) CheckSum(_ context.Context, repo transaction.Repository) ([]byte, error) {
	mn.Lock()
	defer mn.Unlock()

	checksums, ok := mn.checksums[repo]
	if !ok {
		panic(
			fmt.Sprintf(
				"test setup problem: missing checksums in mock node %s for repo %+v",
				mn.storage, repo,
			),
		)
	}

	if len(checksums) < 1 {
		panic(
			fmt.Sprintf(
				"test setup problem: not enough checksums for mock node %s for repo %+v",
				mn.storage, repo,
			),
		)
	}

	cs := checksums[0]
	mn.checksums[repo] = checksums[1:len(checksums)]

	mn.tb.Logf("mock node %s returning checksum %s for repo %s", mn.storage, cs, repo)

	return []byte(cs), nil
}

func (mn *mockNode) ForwardRPC(_ context.Context, rpc *transaction.RPC) error {
	mn.Lock()
	mn.rpc = rpc
	mn.Unlock()

	return nil
}

func (mn *mockNode) Storage() string { return mn.storage }
