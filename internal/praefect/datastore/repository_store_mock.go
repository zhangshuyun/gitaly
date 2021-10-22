package datastore

import "context"

// MockRepositoryStore allows for mocking a RepositoryStore by parametrizing its behavior. All methods
// default to what could be considered success if not set.
type MockRepositoryStore struct {
	GetGenerationFunc                       func(ctx context.Context, repositoryID int64, storage string) (int, error)
	IncrementGenerationFunc                 func(ctx context.Context, repositoryID int64, primary string, secondaries []string) error
	GetReplicatedGenerationFunc             func(ctx context.Context, repositoryID int64, source, target string) (int, error)
	SetGenerationFunc                       func(ctx context.Context, repositoryID int64, storage, relativePath string, generation int) error
	CreateRepositoryFunc                    func(ctx context.Context, repositoryID int64, virtualStorage, relativePath, replicaPath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error
	SetAuthoritativeReplicaFunc             func(ctx context.Context, virtualStorage, relativePath, storage string) error
	DeleteRepositoryFunc                    func(ctx context.Context, virtualStorage, relativePath string) (string, []string, error)
	DeleteReplicaFunc                       func(ctx context.Context, repositoryID int64, storage string) error
	RenameRepositoryInPlaceFunc             func(ctx context.Context, virtualStorage, relativePath, newRelativePath string) error
	RenameRepositoryFunc                    func(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error
	GetConsistentStoragesByRepositoryIDFunc func(ctx context.Context, repositoryID int64) (string, map[string]struct{}, error)
	GetConsistentStoragesFunc               func(ctx context.Context, virtualStorage, relativePath string) (string, map[string]struct{}, error)
	GetPartiallyAvailableRepositoriesFunc   func(ctx context.Context, virtualStorage string) ([]RepositoryMetadata, error)
	DeleteInvalidRepositoryFunc             func(ctx context.Context, repositoryID int64, storage string) error
	RepositoryExistsFunc                    func(ctx context.Context, virtualStorage, relativePath string) (bool, error)
	ReserveRepositoryIDFunc                 func(ctx context.Context, virtualStorage, relativePath string) (int64, error)
	GetRepositoryIDFunc                     func(ctx context.Context, virtualStorage, relativePath string) (int64, error)
	GetReplicaPathFunc                      func(ctx context.Context, repositoryID int64) (string, error)
	GetRepositoryMetadataFunc               func(ctx context.Context, repositoryID int64) (RepositoryMetadata, error)
	GetRepositoryMetadataByPathFunc         func(ctx context.Context, virtualStorage, relativePath string) (RepositoryMetadata, error)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) GetGeneration(ctx context.Context, repositoryID int64, storage string) (int, error) {
	if m.GetGenerationFunc == nil {
		return GenerationUnknown, nil
	}

	return m.GetGenerationFunc(ctx, repositoryID, storage)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) IncrementGeneration(ctx context.Context, repositoryID int64, primary string, secondaries []string) error {
	if m.IncrementGenerationFunc == nil {
		return nil
	}

	return m.IncrementGenerationFunc(ctx, repositoryID, primary, secondaries)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) GetReplicatedGeneration(ctx context.Context, repositoryID int64, source, target string) (int, error) {
	if m.GetReplicatedGenerationFunc == nil {
		return GenerationUnknown, nil
	}

	return m.GetReplicatedGenerationFunc(ctx, repositoryID, source, target)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) SetGeneration(ctx context.Context, repositoryID int64, storage, relativePath string, generation int) error {
	if m.SetGenerationFunc == nil {
		return nil
	}

	return m.SetGenerationFunc(ctx, repositoryID, storage, relativePath, generation)
}

// CreateRepository calls the mocked function. If no mock has been provided, it returns a nil error.
func (m MockRepositoryStore) CreateRepository(ctx context.Context, repositoryID int64, virtualStorage, relativePath, replicaPath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error {
	if m.CreateRepositoryFunc == nil {
		return nil
	}

	return m.CreateRepositoryFunc(ctx, repositoryID, virtualStorage, relativePath, replicaPath, primary, updatedSecondaries, outdatedSecondaries, storePrimary, storeAssignments)
}

// SetAuthoritativeReplica calls the mocked function. If no mock has been provided, it returns a nil error.
func (m MockRepositoryStore) SetAuthoritativeReplica(ctx context.Context, virtualStorage, relativePath, storage string) error {
	if m.SetAuthoritativeReplicaFunc == nil {
		return nil
	}

	return m.SetAuthoritativeReplicaFunc(ctx, virtualStorage, relativePath, storage)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) DeleteRepository(ctx context.Context, virtualStorage, relativePath string) (string, []string, error) {
	if m.DeleteRepositoryFunc == nil {
		return "", nil, nil
	}

	return m.DeleteRepositoryFunc(ctx, virtualStorage, relativePath)
}

// DeleteReplica runs the mock's DeleteReplicaFunc.
func (m MockRepositoryStore) DeleteReplica(ctx context.Context, repositoryID int64, storage string) error {
	if m.DeleteReplicaFunc == nil {
		return nil
	}

	return m.DeleteReplicaFunc(ctx, repositoryID, storage)
}

// RenameRepositoryInPlace runs the mock's RenameRepositoryInPlaceFunc.
func (m MockRepositoryStore) RenameRepositoryInPlace(ctx context.Context, virtualStorage, relativePath, newRelativePath string) error {
	return m.RenameRepositoryInPlaceFunc(ctx, virtualStorage, relativePath, newRelativePath)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error {
	if m.RenameRepositoryFunc == nil {
		return nil
	}

	return m.RenameRepositoryFunc(ctx, virtualStorage, relativePath, storage, newRelativePath)
}

// GetConsistentStoragesByRepositoryID returns result of execution of the GetConsistentStoragesByRepositoryIDFunc field if it is set or an empty map.
func (m MockRepositoryStore) GetConsistentStoragesByRepositoryID(ctx context.Context, repositoryID int64) (string, map[string]struct{}, error) {
	if m.GetConsistentStoragesFunc == nil {
		return "", map[string]struct{}{}, nil
	}

	return m.GetConsistentStoragesByRepositoryIDFunc(ctx, repositoryID)
}

// GetConsistentStorages returns result of execution of the GetConsistentStoragesFunc field if it is set or an empty map.
func (m MockRepositoryStore) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (string, map[string]struct{}, error) {
	if m.GetConsistentStoragesFunc == nil {
		return "", map[string]struct{}{}, nil
	}

	return m.GetConsistentStoragesFunc(ctx, virtualStorage, relativePath)
}

// GetPartiallyAvailableRepositories returns the result of GetPartiallyAvailableRepositories or nil if it is unset.
func (m MockRepositoryStore) GetPartiallyAvailableRepositories(ctx context.Context, virtualStorage string) ([]RepositoryMetadata, error) {
	if m.GetPartiallyAvailableRepositoriesFunc == nil {
		return nil, nil
	}

	return m.GetPartiallyAvailableRepositoriesFunc(ctx, virtualStorage)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) DeleteInvalidRepository(ctx context.Context, repositoryID int64, storage string) error {
	if m.DeleteInvalidRepositoryFunc == nil {
		return nil
	}

	return m.DeleteInvalidRepositoryFunc(ctx, repositoryID, storage)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m MockRepositoryStore) RepositoryExists(ctx context.Context, virtualStorage, relativePath string) (bool, error) {
	if m.RepositoryExistsFunc == nil {
		return true, nil
	}

	return m.RepositoryExistsFunc(ctx, virtualStorage, relativePath)
}

// ReserveRepositoryID returns the result of ReserveRepositoryIDFunc or 0 if it is unset.
func (m MockRepositoryStore) ReserveRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error) {
	if m.ReserveRepositoryIDFunc == nil {
		return 0, nil
	}

	return m.ReserveRepositoryIDFunc(ctx, virtualStorage, relativePath)
}

// GetRepositoryID returns the result of GetRepositoryIDFunc or 0 if it is unset.
func (m MockRepositoryStore) GetRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error) {
	if m.GetRepositoryIDFunc == nil {
		return 0, nil
	}

	return m.GetRepositoryIDFunc(ctx, virtualStorage, relativePath)
}

// GetReplicaPath returns the result of GetReplicaPathFunc or panics if it is unset.
func (m MockRepositoryStore) GetReplicaPath(ctx context.Context, repositoryID int64) (string, error) {
	return m.GetReplicaPathFunc(ctx, repositoryID)
}

// GetRepositoryMetadata returns the result of GetRepositoryMetadataFunc or panics if it is unset.
func (m MockRepositoryStore) GetRepositoryMetadata(ctx context.Context, repositoryID int64) (RepositoryMetadata, error) {
	return m.GetRepositoryMetadataFunc(ctx, repositoryID)
}

// GetRepositoryMetadataByPath returns the result of GetRepositoryMetadataByPathFunc or panics if it is unset.
func (m MockRepositoryStore) GetRepositoryMetadataByPath(ctx context.Context, virtualStorage, relativePath string) (RepositoryMetadata, error) {
	return m.GetRepositoryMetadataByPathFunc(ctx, virtualStorage, relativePath)
}
