package config

import (
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// tmpRootPrefix is the directory in which we store temporary
	// directories.
	tmpRootPrefix = GitalyDataPrefix + "/tmp"

	// cachePrefix is the directory where all cache data is stored on a
	// storage location.
	cachePrefix = GitalyDataPrefix + "/cache"

	// statePrefix is the directory where all state data is stored on a
	// storage location.
	statePrefix = GitalyDataPrefix + "/state"
)

// NewLocator returns locator based on the provided configuration struct.
// As it creates a shallow copy of the provided struct changes made into provided struct
// may affect result of methods implemented by it.
func NewLocator(conf Cfg) storage.Locator {
	return &configLocator{conf: conf}
}

type configLocator struct {
	conf Cfg
}

// GetRepoPath returns the full path of the repository referenced by an
// RPC Repository message. It verifies the path is an existing git directory.
// The errors returned are gRPC errors with relevant error codes and should
// be passed back to gRPC without further decoration.
func (l *configLocator) GetRepoPath(repo repository.GitRepo) (string, error) {
	repoPath, err := l.GetPath(repo)
	if err != nil {
		return "", err
	}

	if repoPath == "" {
		return "", status.Errorf(codes.InvalidArgument, "GetRepoPath: empty repo path")
	}

	if storage.IsGitDirectory(repoPath) {
		return repoPath, nil
	}

	return "", status.Errorf(codes.NotFound, "GetRepoPath: not a git repository: %q", repoPath)
}

// GetPath returns the path of the repo passed as first argument. An error is
// returned when either the storage can't be found or the path includes
// constructs trying to perform directory traversal.
func (l *configLocator) GetPath(repo repository.GitRepo) (string, error) {
	storagePath, err := l.GetStorageByName(repo.GetStorageName())
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(storagePath); err != nil {
		if os.IsNotExist(err) {
			return "", status.Errorf(codes.NotFound, "GetPath: does not exist: %v", err)
		}
		return "", status.Errorf(codes.Internal, "GetPath: storage path: %v", err)
	}

	relativePath := repo.GetRelativePath()
	if len(relativePath) == 0 {
		err := status.Errorf(codes.InvalidArgument, "GetPath: relative path missing from %+v", repo)
		return "", err
	}

	if _, err := storage.ValidateRelativePath(storagePath, relativePath); err != nil {
		return "", status.Errorf(codes.InvalidArgument, "GetRepoPath: %s", err)
	}

	return filepath.Join(storagePath, relativePath), nil
}

// GetStorageByName will return the path for the storage, which is fetched by
// its key. An error is return if it cannot be found.
func (l *configLocator) GetStorageByName(storageName string) (string, error) {
	storagePath, ok := l.conf.StoragePath(storageName)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "GetStorageByName: no such storage: %q", storageName)
	}

	return storagePath, nil
}

// CacheDir returns the path to the cache dir for a storage.
func (l *configLocator) CacheDir(storageName string) (string, error) {
	return l.getPath(storageName, cachePrefix)
}

// StateDir returns the path to the state dir for a storage.
func (l *configLocator) StateDir(storageName string) (string, error) {
	return l.getPath(storageName, statePrefix)
}

// TempDir returns the path to the temp dir for a storag.
func (l *configLocator) TempDir(storageName string) (string, error) {
	return l.getPath(storageName, tmpRootPrefix)
}

func (l *configLocator) getPath(storageName, prefix string) (string, error) {
	storagePath, ok := l.conf.StoragePath(storageName)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "%s dir: no such storage: %q",
			filepath.Base(prefix), storageName)
	}

	return filepath.Join(storagePath, prefix), nil
}
