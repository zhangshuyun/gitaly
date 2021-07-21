package config

import (
	"os"
	"path/filepath"
	"strings"

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

// GetObjectDirectoryPath returns the full path of the object directory in a
// repository referenced by an RPC Repository message. The errors returned are
// gRPC errors with relevant error codes and should be passed back to gRPC
// without further decoration.
func (l *configLocator) GetObjectDirectoryPath(repo repository.GitRepo) (string, error) {
	repoPath, err := l.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	objectDirectoryPath := repo.GetGitObjectDirectory()
	if objectDirectoryPath == "" {
		return "", status.Errorf(codes.InvalidArgument, "GetObjectDirectoryPath: empty directory")
	}

	// We need to check whether the relative object directory as given by the repository is
	// a valid path. This may either be a path in the Git repository itself, where it may either
	// point to the main object directory storage or to an object quarantine directory as
	// created by git-receive-pack(1). Alternatively, if that is not the case, then it may be a
	// manual object quarantine directory located in the storage's temporary directory. These
	// have a repository-specific prefix which we must check in order to determine whether the
	// quarantine directory does in fact belong to the repo at hand.
	if _, origError := storage.ValidateRelativePath(repoPath, objectDirectoryPath); origError != nil {
		tempDir, err := l.TempDir(repo.GetStorageName())
		if err != nil {
			return "", status.Errorf(codes.InvalidArgument, "GetObjectDirectoryPath: %s", err)
		}

		expectedQuarantinePrefix := filepath.Join(tempDir, storage.QuarantineDirectoryPrefix(repo))
		absoluteObjectDirectoryPath := filepath.Join(repoPath, objectDirectoryPath)

		if !strings.HasPrefix(absoluteObjectDirectoryPath, expectedQuarantinePrefix) {
			return "", status.Errorf(codes.InvalidArgument, "GetObjectDirectoryPath: %s", origError)
		}
	}

	fullPath := filepath.Join(repoPath, objectDirectoryPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return "", status.Errorf(codes.NotFound, "GetObjectDirectoryPath: does not exist: %q", fullPath)
	}

	return fullPath, nil
}

func (l *configLocator) InfoAlternatesPath(repo repository.GitRepo) (string, error) {
	repoPath, err := l.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	return filepath.Join(repoPath, "objects", "info", "alternates"), nil
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
