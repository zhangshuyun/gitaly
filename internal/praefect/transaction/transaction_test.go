package transaction_test

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/praefect"
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

	// A replication manager needs to have the ability to verify the state of
	// replicas, so it needs a Verifier.
	rm := praefect.NewReplicationManager(mv)

	// replication managers are typically used within the context of a request
	// when a mutator RPC is received.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

}

type mockVerifier struct {
	// checksums contains ordered checksums keyed by project and then storage
	checksums map[string]map[string][][]byte
}

func (mv *mockVerifier) CheckSum(_ context.Context, project, storage string) ([]byte, error) {
	storages, ok := mv.checksums[project]
	if !ok {
		panic("no project " + project)
	}

	sums, ok := storages[storage]
	if !ok {
		panic("no storage " + storage)
	}

	if len(sums) < 1 {
		panic("no more checksums for " + project)
	}

	// pop first checksum off list
	var sum []byte
	sum, mv.checksums[project][storage] = sums[len(sums)-1], sums[:len(sums)-1]

	return sum, nil
}
