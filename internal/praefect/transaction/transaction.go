package transaction

import (
	"context"
	"sync"
)

// Repository represents the identity and location of a repository as requested
// by the client
type Repository struct {
	ProjectHash string
	StorageLoc  string // storage location
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

	CheckSum(context.Context, Repository) ([]byte, error)
	WriteRef(ctx context.Context, ref, value string) error
}

// MutateTx represents the resources available during a mutator transaction
type MutateTx interface {
	AccessTx
	// LockRef acquires a write lock on a reference
	LockRef(context.Context, string) error
	Primary() Node
}

// AccessTx represents the resources available during an accessor transaction
type AccessTx interface {
	// Replicas returns all replicas without distinguishing the primary
	Replicas() map[string]Node
	// RLockRef acquires a read lock on a reference
	RLockRef(context.Context, string) error
}

type transaction struct {
	shard *Shard

	replicas map[string]Node // only nodes verified to be consistent

	// unlocks contains callbacks to unlock all locks acquired
	unlocks struct {
		*sync.RWMutex
		m map[string]func()
	}

	// refRollbacks contains all reference values before modification
	refRollbacks map[string][]byte
}

func newTransaction(shard *Shard, good []Node) transaction {
	replicas := map[string]Node{}
	for _, node := range good {
		replicas[node.Storage()] = node
	}

	return transaction{
		shard:    shard,
		replicas: replicas,
		unlocks: struct {
			*sync.RWMutex
			m map[string]func()
		}{
			RWMutex: new(sync.RWMutex),
			m:       make(map[string]func()),
		},
	}
}

// LockRef attempts to acquire a write lock on the reference name provided.
// If the context is cancelled first, the lock acquisition will be
// aborted.
func (t transaction) LockRef(ctx context.Context, ref string) error {
	lockQ := make(chan *sync.RWMutex)

	go func() {
		t.shard.refLocks.RLock()
		l, ok := t.shard.refLocks.m[ref]
		t.shard.refLocks.RUnlock()

		if !ok {
			l = new(sync.RWMutex)
			t.shard.refLocks.Lock()
			t.shard.refLocks.m[ref] = l
			t.shard.refLocks.Unlock()
		}

		l.Lock()
		lockQ <- l
	}()

	// ensure lockQ is consumed in all code paths so that goroutine doesn't
	// stray
	select {

	case <-ctx.Done():
		l := <-lockQ
		l.Unlock()
		return ctx.Err()

	case l := <-lockQ:
		t.unlocks.Lock()
		t.unlocks.m[ref] = func() { l.Unlock() }
		t.unlocks.Unlock()

		return nil
	}
}

// unlockAll will unlock all acquired locks at the end of a transaction
func (t transaction) unlockAll() {
	// unlock all refs
	t.unlocks.RLock()
	for _, unlock := range t.unlocks.m {
		unlock()
	}
	t.unlocks.RUnlock()
}

func (t transaction) rollback(ctx context.Context) error {
	// for ref, value := range t.refRollbacks {
	//
	// }
	return nil
}

func (t transaction) Replicas() map[string]Node {
	return t.replicas
}

func (t transaction) Primary() Node {
	return t.replicas[t.shard.primary]
}

// RLockRef attempts to acquire a read lock on the reference name provided
// (ref). If the context is cancelled first, the lock acquisition will be
// aborted.
func (t transaction) RLockRef(ctx context.Context, ref string) error {
	lockQ := make(chan *sync.RWMutex)

	go func() {
		t.shard.refLocks.RLock()
		l, ok := t.shard.refLocks.m[ref]
		t.shard.refLocks.RUnlock()

		if !ok {
			l = new(sync.RWMutex)
			t.shard.refLocks.Lock()
			t.shard.refLocks.m[ref] = l
			t.shard.refLocks.Unlock()
		}

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
		t.unlocks.Lock()
		t.unlocks.m[ref] = func() { l.RUnlock() }
		t.unlocks.Unlock()

		return nil

	}
}
