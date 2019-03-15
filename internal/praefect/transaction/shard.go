package transaction

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Shard represents a set of Gitaly replicas for a repository. Each shard has a
// designated primary and maintains locks to resources that can cause contention
// between clients writing to the same repo.
type Shard struct {
	lock            *sync.RWMutex   // all mutations require a write lock
	repo            Repository      // the repo this replica backs
	primary         string          // the storage location of the primary node
	storageReplicas map[string]Node // maps storage location to a replica
}

func NewShard(r Repository, primary string, replicas []Node) *Shard {
	sreps := make(map[string]Node)
	for _, r := range replicas {
		sreps[r.Storage()] = r
	}

	return &Shard{
		repo:            r,
		primary:         primary,
		storageReplicas: sreps,
		lock:            new(sync.RWMutex),
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

	for storage, node := range s.storageReplicas {
		_, ok := omits[storage]
		if ok {
			continue
		}

		storage := storage // rescope iterator vars

		eg.Go(func() error {
			cs, err := node.CheckSum(eCtx, s.repo)
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
