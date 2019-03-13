/*Package praefect provides transaction management functionality to coordinate
one-to-many clients attempting to modify the shards concurrently.

While Git is distributed in nature, there are some repository wide data points
that can conflict between replicas if something goes wrong. This includes
references, which is why the transaction manager provides an API that allows
an RPC transaction to read/write lock the references being accessed to prevent
contention.
*/
package praefect

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Repository represents the identity and location of a repository as requested
// by the client
type Repository struct {
	ProjectHash string
	StorageLoc  string // storage location
}

// Verifier verifies the project repository state
type Verifier interface {
	// CheckSum will return the checksum for a project in a storage location
	CheckSum(context.Context, Repository) ([]byte, error)
}

// ReplicationManager provides services to handle degraded nodes
type ReplicationManager interface {
	NotifyDegradation(context.Context, Repository, Node) error
}

// TransactionManager tracks the progress of RPCs being applied to multiple
// downstream servers that make up a shard. It prevents conflicts that may arise
// from contention between multiple clients trying to modify the same
// references.
type TransactionManager struct {
	mu     sync.Mutex
	shards map[string]*Shard // shards keyed by project

	verifier    Verifier
	coordinator Coordinator
	replman     ReplicationManager
}

func NewTransactionManager(v Verifier, c Coordinator, r ReplicationManager) *TransactionManager {
	return &TransactionManager{
		shards:      map[string]*Shard{},
		verifier:    v,
		coordinator: c,
		replman:     r,
	}
}

// State represents the current state of a backend node
// Note: in the future this may be extended to include refs
type State struct {
	Checksum []byte
}

type RPC struct {
	// @zj how do we abstract an RPC invocation? 🤔
}

type Node interface {
	Storage() string // storage location the node hosts
	ForwardRPC(ctx context.Context, rpc *RPC) error
}

// Shard represents
type Shard struct {
	repo Repository

	primary string // the designated primary node name

	// Replicas maps a storage location to the node replicas
	storageReplicas map[string]Node

	// refLocks coordinates changes between many clients attempting to mutate
	// a reference
	refLocks map[string]*sync.RWMutex

	// used to check shard for inconsistencies
	verifier Verifier
}

// ErrShardInconsistent indicates a mutating operation is unable to be executed
// due to lack of quorum of consistent nodes.
var ErrShardInconsistent = errors.New("majority of shard nodes are in inconsistent state")

func (s Shard) validate(ctx context.Context) (good, bad []Node, err error) {
	var (
		mu        sync.RWMutex
		checksums = map[string][]byte{}
	)

	eg, eCtx := errgroup.WithContext(ctx)

	for storage, _ := range s.storageReplicas {
		storage := storage // rescope iterator vars

		eg.Go(func() error {
			cs, err := s.verifier.CheckSum(eCtx, s.repo)
			if err != nil {
				return err
			}

			mu.Lock()
			checksums[storage] = cs
			mu.Unlock()

			return nil
		})

	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	pCS := string(checksums[s.primary])
	for storage, cs := range checksums {
		n := s.storageReplicas[storage]
		if string(cs) != pCS {
			bad = append(bad, n)
			continue
		}
		good = append(good, n)
	}

	return good, bad, nil
}

// MutateTx represents the resources available during a mutator transaction
type MutateTx interface {
	AccessTx
	// LockRef acquires a write lock on a reference
	LockRef(context.Context, string) error
	Shard() *Shard
}

// AccessTx represents the resources available during an accessor transaction
type AccessTx interface {
	// Replicas returns all replicas without distinguishing the primary
	Replicas() map[string]Node
	// RLockRef acquires a read lock on a reference
	RLockRef(context.Context, string) error
}

type transaction struct {
	shard    *Shard
	refLocks map[string]*sync.RWMutex
}

func (t transaction) Shard() *Shard { return t.shard }

// LockRef attempts to acquire a write lock on the reference name provided
//  (ref). If the context is cancelled first, the lock acquisition will be
// aborted.
func (t transaction) LockRef(ctx context.Context, ref string) error {
	lockQ := make(chan *sync.RWMutex)

	go func() {
		l := t.shard.refLocks[ref]
		l.Lock()
		lockQ <- l
	}()

	// ensure lockQ is consumed in all code paths so that goroutine doesn't
	// stray
	select {

	case <-ctx.Done():
		// unlock before aborting
		l := <-lockQ
		l.Unlock()

		return ctx.Err()

	case l := <-lockQ:
		t.refLocks[ref] = l

	}

	return nil
}

// unlockAll will unlock all acquired locks at the end of a transaction
func (t transaction) unlockAll() {
	// unlock all refs
	for _, rl := range t.refLocks {
		rl.Unlock()
	}
}

func (t transaction) Replicas() map[string]Node {
	return t.shard.storageReplicas
}

// RLockRef attempts to acquire a read lock on the reference name provided
// (ref). If  the context is cancelled first, the lock acquisition will be
// aborted.
func (t transaction) RLockRef(ctx context.Context, ref string) error {
	lockQ := make(chan *sync.RWMutex)

	go func() {
		l := t.shard.refLocks[ref]
		l.RLock()
		lockQ <- l
	}()

	// ensure lockQ is consumed in all code paths so that goroutine doesn't
	// stray
	select {

	case <-ctx.Done():
		// unlock before aborting
		l := <-lockQ
		l.RUnlock()

		return ctx.Err()

	case l := <-lockQ:
		t.refLocks[ref] = l

	}

	return nil
}

// Mutate accepts a closure whose environment is a snapshot of the repository
// before the transaction begins. It is the responsibility of the closure
// author to mark all relevant assets being modified during a mutating
// transaction to ensure they are locked and protected from other closures
// modifying the same.
func (tm *TransactionManager) Mutate(ctx context.Context, repo Repository, fn func(MutateTx) error) error {
	shard, err := tm.coordinator.FetchShard(ctx, repo)
	if err != nil {
		return err
	}

	good, bad, err := shard.validate(ctx)
	if err != nil {
		return err
	}

	// are a majority of the nodes good (consistent)?
	if len(bad) >= len(good) {
		return ErrShardInconsistent
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for _, node := range bad {
		node := node // rescope iterator var for goroutine closure

		eg.Go(func() error {
			return tm.replman.NotifyDegradation(eCtx, repo, node)
		})
	}

	tx := transaction{
		shard:    shard,
		refLocks: map[string]*sync.RWMutex{},
	}
	defer tx.unlockAll()

	// run the transaction function
	err = fn(tx)
	if err != nil {
		return err
	}

	return nil
}

func (tm *TransactionManager) Access(ctx context.Context, repo Repository, fn func(AccessTx) error) error {
	shard, err := tm.coordinator.FetchShard(ctx, repo)
	if err != nil {
		return err
	}

	good, bad, err := shard.validate(ctx)
	if err != nil {
		return err
	}

	// are a majority of the nodes good (consistent)?
	if len(bad) >= len(good) {
		return ErrShardInconsistent
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for _, node := range bad {
		node := node // rescope iterator var for goroutine closure

		eg.Go(func() error {
			return tm.replman.NotifyDegradation(eCtx, repo, node)
		})
	}

	tx := transaction{
		shard:    shard,
		refLocks: map[string]*sync.RWMutex{},
	}
	defer tx.unlockAll()

	// run the transaction function
	err = fn(tx)
	if err != nil {
		return err
	}

	return nil
}

func (c *Coordinator) FetchShard(ctx context.Context, repo Repository) (*Shard, error) {
	// TODO: move this to coordinator.go
	return nil, nil
}
