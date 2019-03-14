package transaction

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Shard represents a set of Gitaly replicas for repository. Each shard has a
// designated primary and maintains locks to resources that can cause contention
// between clients writing to the same repo.
type Shard struct {
	repo Repository

	primary string // the designated primary node name

	storageReplicas map[string]Node // maps storage location to a replica

	refLocks struct {
		*sync.RWMutex
		m map[string]*sync.RWMutex // maps ref name to a lock
	}

	// used to check shard for inconsistencies
	verifier Verifier
}

func NewShard(r Repository, primary string, replicas []Node, v Verifier) *Shard {
	return &Shard{
		repo:            r,
		primary:         primary,
		storageReplicas: make(map[string]Node),
		refLocks: struct {
			*sync.RWMutex
			m map[string]*sync.RWMutex
		}{
			RWMutex: new(sync.RWMutex),
			m:       make(map[string]*sync.RWMutex),
		},
		verifier: v,
	}
}

// ErrShardInconsistent indicates a mutating operation is unable to be executed
// due to lack of quorum of consistent nodes.
var ErrShardInconsistent = errors.New("majority of shard nodes are in inconsistent state")

// validate will concurrently fetch the checksum of each node in the shard and
// compare against the primary. All replicas consistent with the primary will
// be returned as "good", and the rest "bad". If only a partial check needs to
// be done, a list of Node storage keys can be provided to exclude those from
// the checks.
func (s Shard) validate(ctx context.Context, omits map[string]struct{}) (good, bad []Node, err error) {
	var (
		mu        sync.RWMutex
		checksums = map[string][]byte{}
	)

	eg, eCtx := errgroup.WithContext(ctx)

	for storage, _ := range s.storageReplicas {
		_, ok := omits[storage]
		if ok {
			continue
		}

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
