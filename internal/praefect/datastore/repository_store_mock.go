package datastore

import "context"

// MockRepositoryStore allows for mocking a RepositoryStore by parametrizing its behavior. All methods
// default to what could be considered success if not set.
type MockRepositoryStore struct {
	GetGenerationFunc                      func(ctx context.Context, virtualStorage, relativePath, storage string) (int, error)
	IncrementGenerationFunc                func(ctx context.Context, virtualStorage, relativePath, primary string, secondaries []string) error
	GetReplicatedGenerationFunc            func(ctx context.Context, virtualStorage, relativePath, source, target string) (int, error)
	SetGenerationFunc                      func(ctx context.Context, virtualStorage, relativePath, storage string, generation int) error
	CreateRepositoryFunc                   func(ctx context.Context, virtualStorage, relativePath, storage string, storePrimary bool) error
	DeleteRepositoryFunc                   func(ctx context.Context, virtualStorage, relativePath, storage string) error
	RenameRepositoryFunc                   func(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error
	GetConsistentStoragesFunc              func(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error)
	GetPartiallyReplicatedRepositoriesFunc func(ctx context.Context, virtualStorage string, virtualStorageScopedPrimaries bool) ([]OutdatedRepository, error)
	DeleteInvalidRepositoryFunc            func(ctx context.Context, virtualStorage, relativePath, storage string) error
	RepositoryExistsFunc                   func(ctx context.Context, virtualStorage, relativePath string) (bool, error)
}

func (m MockRepositoryStore) GetGeneration(ctx context.Context, virtualStorage, relativePath, storage string) (int, error) {
	if m.GetGenerationFunc == nil {
		return GenerationUnknown, nil
	}

	return m.GetGenerationFunc(ctx, virtualStorage, relativePath, storage)
}

func (m MockRepositoryStore) IncrementGeneration(ctx context.Context, virtualStorage, relativePath, primary string, secondaries []string) error {
	if m.IncrementGenerationFunc == nil {
		return nil
	}

	return m.IncrementGenerationFunc(ctx, virtualStorage, relativePath, primary, secondaries)
}

func (m MockRepositoryStore) GetReplicatedGeneration(ctx context.Context, virtualStorage, relativePath, source, target string) (int, error) {
	if m.GetReplicatedGenerationFunc == nil {
		return GenerationUnknown, nil
	}

	return m.GetReplicatedGenerationFunc(ctx, virtualStorage, relativePath, source, target)
}

func (m MockRepositoryStore) SetGeneration(ctx context.Context, virtualStorage, relativePath, storage string, generation int) error {
	if m.SetGenerationFunc == nil {
		return nil
	}

	return m.SetGenerationFunc(ctx, virtualStorage, relativePath, storage, generation)
}

//nolint:stylecheck
//nolint:golint
func (m MockRepositoryStore) CreateRepository(ctx context.Context, virtualStorage, relativePath, storage string, storePrimary bool) error {
	if m.CreateRepositoryFunc == nil {
		return nil
	}

	return m.CreateRepositoryFunc(ctx, virtualStorage, relativePath, storage, storePrimary)
}

func (m MockRepositoryStore) DeleteRepository(ctx context.Context, virtualStorage, relativePath, storage string) error {
	if m.DeleteRepositoryFunc == nil {
		return nil
	}

	return m.DeleteRepositoryFunc(ctx, virtualStorage, relativePath, storage)
}

func (m MockRepositoryStore) RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error {
	if m.RenameRepositoryFunc == nil {
		return nil
	}

	return m.RenameRepositoryFunc(ctx, virtualStorage, relativePath, storage, newRelativePath)
}

// GetConsistentStorages returns result of execution of the GetConsistentStoragesFunc field if it is set or an empty map.
func (m MockRepositoryStore) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
	if m.GetConsistentStoragesFunc == nil {
		return map[string]struct{}{}, nil
	}

	return m.GetConsistentStoragesFunc(ctx, virtualStorage, relativePath)
}

func (m MockRepositoryStore) GetPartiallyReplicatedRepositories(ctx context.Context, virtualStorage string, virtualStorageScopedPrimaries bool) ([]OutdatedRepository, error) {
	if m.GetPartiallyReplicatedRepositoriesFunc == nil {
		return nil, nil
	}

	return m.GetPartiallyReplicatedRepositoriesFunc(ctx, virtualStorage, virtualStorageScopedPrimaries)
}

func (m MockRepositoryStore) DeleteInvalidRepository(ctx context.Context, virtualStorage, relativePath, storage string) error {
	if m.DeleteInvalidRepositoryFunc == nil {
		return nil
	}

	return m.DeleteInvalidRepositoryFunc(ctx, virtualStorage, relativePath, storage)
}

func (m MockRepositoryStore) RepositoryExists(ctx context.Context, virtualStorage, relativePath string) (bool, error) {
	if m.RepositoryExistsFunc == nil {
		return true, nil
	}

	return m.RepositoryExistsFunc(ctx, virtualStorage, relativePath)
}
