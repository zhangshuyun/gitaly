package transaction_test

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/transaction"
)

func TestReplMan(t *testing.T) {
	const (
		projA = "project-A"
		stor1 = "storage-1"
		stor2 = "storage-2"
		stor3 = "storage-3"
	)

	mv := &mockVerifier{
		checksums: map[string]map[string][][]byte{
			projA: map[string][][]byte{
				stor1: {[]byte{1}},
				stor2: {[]byte{1}},
				stor3: {[]byte{1}},
			},
		},
	}

	// A transaction manager needs to have the ability to verify the state of
	// replicas, so it needs a Verifier.
	rm := transaction.NewManager(mv, mockCoordinator{}, mockReplMan{})
	rm.Access(context.Background(), transaction.Repository{}, func(transaction.AccessTx) error {
		return nil
	})
}

type mockCoordinator struct{}

func (_ mockCoordinator) FetchShard(context.Context, transaction.Repository) (*transaction.Shard, error) {
	return nil, nil
}

type mockReplMan struct{}

func (_ mockReplMan) NotifyDegradation(context.Context, transaction.Repository) error { return nil }

type mockVerifier struct {
	// checksums contains ordered checksums keyed by project and then storage
	checksums map[string]map[string][][]byte
}

func (mv *mockVerifier) CheckSum(_ context.Context, repo transaction.Repository) ([]byte, error) {
	storages, ok := mv.checksums[repo.ProjectHash]
	if !ok {
		panic("no project " + repo.ProjectHash)
	}

	sums, ok := storages[repo.StorageLoc]
	if !ok {
		panic("no storage " + repo.StorageLoc)
	}

	if len(sums) < 1 {
		panic("no more checksums for " + repo.ProjectHash)
	}

	// pop first checksum off list
	var sum []byte
	sum, mv.checksums[repo.ProjectHash][repo.StorageLoc] = sums[len(sums)-1], sums[:len(sums)-1]

	return sum, nil
}
