package transaction

import (
	"context"
	"errors"
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
}

type txCategory int

const (
	txCatAccessor = iota
	txCatMutator
)

type Tx struct {
	category txCategory

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

func newTx(shard *Shard, good []Node, txCat txCategory) Tx {
	replicas := map[string]Node{}
	for _, node := range good {
		replicas[node.Storage()] = node
	}

	return Tx{
		category: txCat,
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

func (t Tx) rollback(ctx context.Context) error {
	// for ref, value := range t.refRollbacks {
	//
	// }
	return nil
}

func (t Tx) Replicas() map[string]Node {
	return t.replicas
}

var ErrAccessorNotPermitted = errors.New("a mutator operation was attempted by an accessor")

func (t Tx) Primary() (Node, error) {
	if t.category != txCatMutator {
		return nil, ErrAccessorNotPermitted
	}

	return t.replicas[t.shard.primary], nil
}
