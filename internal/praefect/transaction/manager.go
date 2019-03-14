package transaction

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Verifier verifies the project repository state by performing a checksum
type Verifier interface {
	// CheckSum will return checksum of all refs for a repo
	CheckSum(context.Context, Repository) ([]byte, error)
}

// Coordinator allows the transaction manager to look up the shard for a repo
// at the beginning of each transaction
type Coordinator interface {
	FetchShard(ctx context.Context, repo Repository) (*Shard, error)
}

// ReplicationManager provides services to handle degraded nodes
type ReplicationManager interface {
	NotifyDegradation(context.Context, Repository) error
}

// Manager tracks the progress of RPCs being applied to multiple
// downstream servers that make up a shard. It prevents conflicts that may arise
// from contention between multiple clients trying to modify the same
// references.
type Manager struct {
	mu     sync.Mutex
	shards map[string]*Shard // shards keyed by project

	verifier    Verifier
	coordinator Coordinator
	replman     ReplicationManager
}

// NewManafer returns a manager for coordinating transaction between shards
// that are fetched from the coordinator. Any inconsistencies found will be
// reported to the provided replication manager.
func NewManager(v Verifier, c Coordinator, r ReplicationManager) *Manager {
	return &Manager{
		shards:      map[string]*Shard{},
		verifier:    v,
		coordinator: c,
		replman:     r,
	}
}

// Mutate accepts a closure whose environment is a snapshot of the repository
// before the transaction begins. It is the responsibility of the closure
// author to mark all relevant assets being modified during a mutating
// transaction to ensure they are locked and protected from other closures
// modifying the same.
func (m *Manager) Mutate(ctx context.Context, repo Repository, fn func(MutateTx) error) error {
	shard, err := m.coordinator.FetchShard(ctx, repo)
	if err != nil {
		return err
	}

	omits := make(map[string]struct{})

	// TODO: some smart caching needs to be done to eliminate this check. We
	// already check the shard consistency at the end of each transaction so
	// we shouldn't need to do it twice except for the first time we fetch a
	// shard.
	good, bad, err := shard.validate(ctx, omits)
	if err != nil {
		return err
	}

	// are a majority of the nodes good (consistent)?
	if len(bad) >= len(good) {
		return ErrShardInconsistent
	}

	omits, err = m.notifyDegradations(ctx, repo, bad)
	if err != nil {
		return err
	}

	tx := newTransaction(shard, good)
	defer tx.unlockAll()

	err = fn(tx)
	if err != nil {
		return err
	}

	// make sure all changes made to replicas are consistent
	good, bad, err = shard.validate(ctx, omits)
	if err != nil {
		return err
	}

	_, err = m.notifyDegradations(ctx, repo, bad)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) notifyDegradations(ctx context.Context, repo Repository, degradeds []Node) (map[string]struct{}, error) {
	reported := make(map[string]struct{})

	eg, eCtx := errgroup.WithContext(ctx)
	for _, node := range degradeds {
		node := node // rescope iterator var for goroutine closure
		reported[node.Storage()] = struct{}{}

		eg.Go(func() error {
			return m.replman.NotifyDegradation(eCtx, repo)
		})
	}

	return reported, eg.Wait()
}

func (m *Manager) Access(ctx context.Context, repo Repository, fn func(AccessTx) error) error {
	shard, err := m.coordinator.FetchShard(ctx, repo)
	if err != nil {
		return err
	}

	// TODO: some smart caching needs to be done to eliminate this check. We
	// already check the shard consistency at the end of each transaction so
	// we shouldn't need to do it twice except for the first time we fetch a
	// shard.
	good, bad, err := shard.validate(ctx, map[string]struct{}{})
	if err != nil {
		return err
	}

	_, err = m.notifyDegradations(ctx, repo, bad)
	if err != nil {
		return err
	}

	tx := newTransaction(shard, good)
	defer tx.unlockAll()

	err = fn(tx)
	if err != nil {
		return err
	}

	return nil
}
