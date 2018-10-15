package helper

import (
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetRepoPath returns the full path of the repository referenced by an
// RPC Repository message. The errors returned are gRPC errors with
// relevant error codes and should be passed back to gRPC without further
// decoration.
func GetRepoPath(repo *gitalypb.Repository) (string, error) {
	repoPath, err := GetPath(repo)
	if err != nil {
		return "", err
	}

	if repoPath == "" {
		return "", status.Errorf(codes.InvalidArgument, "GetRepoPath: empty repo")
	}

	if IsGitDirectory(repoPath) {
		return repoPath, nil
	}

	return "", status.Errorf(codes.NotFound, "GetRepoPath: not a git repository '%s'", repoPath)
}

// GetPath returns the path of the repo passed as first argument. An error is
// returned when either the storage can't be found or the path includes
// constructs trying to perform directory traversal.
func GetPath(repo *gitalypb.Repository) (string, error) {
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

	if ContainsPathTraversal(relativePath) {
		return "", status.Errorf(codes.InvalidArgument, "GetRepoPath: relative path can't contain directory traversal")
	}

	return path.Join(storagePath, relativePath), nil
}

// GetStorageByName will return the path for the storage, which is fetched by
// its key. An error is return if it cannot be found.
func GetStorageByName(storageName string) (string, error) {
	storagePath, ok := config.StoragePath(storageName)
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "Storage can not be found by name '%s'", storageName)
	}

	return storagePath, nil
}

// IsGitDirectory checks if the directory passed as first argument looks like
// a valid git directory.
func IsGitDirectory(dir string) bool {
	if dir == "" {
		return false
	}

	for _, element := range []string{"objects", "refs", "HEAD"} {
		if _, err := os.Stat(path.Join(dir, element)); err != nil {
			return false
		}
	}

	return true
}
