package datastore

import (
	"context"
	"sync"
)

// LocalGenerationStore is an in-memory implementation of GenerationStore.
// Refer to the interface for method documentation.
type LocalGenerationStore struct {
	m sync.Mutex

	storages     map[string][]string
	generations  map[string]map[string]map[string]int
	repositories map[string]map[string]int
}

// NewLocalGenerationStore returns an in-memory implementation of GenerationStore.
func NewLocalGenerationStore(storages map[string][]string) *LocalGenerationStore {
	return &LocalGenerationStore{
		storages:     storages,
		generations:  make(map[string]map[string]map[string]int),
		repositories: make(map[string]map[string]int),
	}
}

func (l *LocalGenerationStore) IncrementGeneration(ctx context.Context, virtualStorage, storage, relativePath string) (int, error) {
	l.m.Lock()
	defer l.m.Unlock()

	nextGen := l.latestGeneration(virtualStorage, relativePath) + 1
	l.setGeneration(virtualStorage, relativePath, storage, nextGen)

	return nextGen, nil
}

func (l *LocalGenerationStore) SetGeneration(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error {
	l.m.Lock()
	defer l.m.Unlock()

	l.setGeneration(virtualStorage, relativePath, storage, generation)

	return nil
}

func (l *LocalGenerationStore) DeleteRecord(ctx context.Context, virtualStorage, storage, relativePath string) error {
	l.m.Lock()
	defer l.m.Unlock()

	vs, ok := l.generations[virtualStorage]
	if !ok {
		return nil
	}

	rel, ok := vs[relativePath]
	if !ok {
		return nil
	}

	delete(rel, storage)

	return nil
}

func (l *LocalGenerationStore) EnsureUpgrade(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error {
	l.m.Lock()
	defer l.m.Unlock()

	if current := l.getGeneration(virtualStorage, relativePath, storage); current != GenerationUnknown && current >= generation {
		return errDowngradeAttempted
	}

	return nil
}

func (l *LocalGenerationStore) GetOutdatedRepositories(ctx context.Context, virtualStorage string) (map[string]map[string]int, error) {
	storages, ok := l.storages[virtualStorage]
	if !ok {
		return nil, errUnknownVirtualStorage
	}

	outdatedRepos := make(map[string]map[string]int)
	repositories, ok := l.repositories[virtualStorage]
	if !ok {
		return outdatedRepos, nil
	}

	for relativePath, latestGeneration := range repositories {
		for _, storage := range storages {
			if gen := l.getGeneration(virtualStorage, relativePath, storage); gen < latestGeneration {
				if outdatedRepos[relativePath] == nil {
					outdatedRepos[relativePath] = make(map[string]int)
				}

				outdatedRepos[relativePath][storage] = latestGeneration - gen
			}
		}
	}

	return outdatedRepos, nil
}

func (l *LocalGenerationStore) latestGeneration(virtualStorage, relativePath string) int {
	vs := l.repositories[virtualStorage]
	if vs == nil {
		return GenerationUnknown
	}

	if latest, ok := vs[relativePath]; ok {
		return latest
	}

	return GenerationUnknown
}

func (l *LocalGenerationStore) getGeneration(virtualStorage, relativePath, storage string) int {
	vs := l.generations[virtualStorage]
	if vs == nil {
		return GenerationUnknown
	}

	rel := vs[relativePath]
	if rel == nil {
		return GenerationUnknown
	}

	if gen, ok := rel[storage]; ok {
		return gen
	}

	return GenerationUnknown
}

func (l *LocalGenerationStore) setGeneration(virtualStorage, relativePath, storage string, generation int) {
	if generation > l.latestGeneration(virtualStorage, relativePath) {
		if l.repositories[virtualStorage] == nil {
			l.repositories[virtualStorage] = make(map[string]int)
		}
		l.repositories[virtualStorage][relativePath] = generation
	}

	vs := l.generations[virtualStorage]
	if vs == nil {
		l.generations[virtualStorage] = map[string]map[string]int{
			relativePath: {
				storage: generation,
			},
		}

		return
	}

	rel := vs[relativePath]
	if rel == nil {
		vs[relativePath] = map[string]int{
			storage: generation,
		}

		return
	}

	rel[storage] = generation
}
