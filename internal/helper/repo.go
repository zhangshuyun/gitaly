package helper

import (
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetRepoPath returns the full path of the repository referenced by an
// RPC Repository message. The errors returned are gRPC errors with
// relevant error codes and should be passed back to gRPC without further
// decoration.
// Deprecated: please use storage.Locator to define the project path.
func GetRepoPath(repo repository.GitRepo) (string, error) {
	repoPath, err := GetPath(repo)
	if err != nil {
		return "", err
	}

	if repoPath == "" {
		return "", status.Errorf(codes.InvalidArgument, "GetRepoPath: empty repo")
	}

	if storage.IsGitDirectory(repoPath) {
		return repoPath, nil
	}

	return "", status.Errorf(codes.NotFound, "GetRepoPath: not a git repository '%s'", repoPath)
}

// RepoPathEqual compares if two repositories are in the same location
func RepoPathEqual(a, b repository.GitRepo) bool {
	return a.GetStorageName() == b.GetStorageName() &&
		a.GetRelativePath() == b.GetRelativePath()
}

// GetPath returns the path of the repo passed as first argument. An error is
// returned when either the storage can't be found or the path includes
// constructs trying to perform directory traversal.
func GetPath(repo repository.GitRepo) (string, error) {
	storagePath, err := GetStorageByName(repo.GetStorageName())
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(storagePath); err != nil {
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
// Deprecated: please use storage.Locator to define the storage path.
func GetStorageByName(storageName string) (string, error) {
	storagePath, ok := config.Config.StoragePath(storageName)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "Storage can not be found by name '%s'", storageName)
	}

	return storagePath, nil
}

// ProtoRepoFromRepo allows object pools and repository abstractions to be used
// in places that require a concrete type
func ProtoRepoFromRepo(repo repository.GitRepo) *gitalypb.Repository {
	return &gitalypb.Repository{
		StorageName:                   repo.GetStorageName(),
		GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
		GitObjectDirectory:            repo.GetGitObjectDirectory(),
		RelativePath:                  repo.GetRelativePath(),
	}
}
