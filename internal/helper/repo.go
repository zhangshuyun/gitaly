package helper

import (
	"context"
	"os"
	"path"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/config"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// GetRepoPath returns the full path of the repository referenced by an
// RPC Repository message. The errors returned are gRPC errors with
// relevant error codes and should be passed back to gRPC without further
// decoration.
func GetRepoPath(repo *pb.Repository) (string, error) {
	repoPath, err := GetPath(repo)
	if err != nil {
		return "", err
	}

	if repoPath == "" {
		return "", grpc.Errorf(codes.InvalidArgument, "GetRepoPath: empty repo")
	}

	if IsGitDirectory(repoPath) {
		return repoPath, nil
	}

	return "", grpc.Errorf(codes.NotFound, "GetRepoPath: not a git repository '%s'", repoPath)
}

// GetPath returns the path of the repo passed as first argument. An error is
// returned when either the storage can't be found or the path includes
// constructs trying to perform directory traversal.
func GetPath(repo *pb.Repository) (string, error) {
	storagePath, ok := config.StoragePath(repo.GetStorageName())
	if !ok {
		return "", grpc.Errorf(codes.InvalidArgument, "GetPath: invalid storage name '%s'", repo.GetStorageName())
	}

	if _, err := os.Stat(storagePath); err != nil {
		return "", grpc.Errorf(codes.Internal, "GetPath: storage path: %v", err)
	}

	relativePath := repo.GetRelativePath()
	if len(relativePath) == 0 {
		err := grpc.Errorf(codes.InvalidArgument, "GetPath: relative path missing from %+v", repo)
		return "", err
	}

	// Disallow directory traversal for security
	separator := string(os.PathSeparator)
	if strings.HasPrefix(relativePath, ".."+separator) ||
		strings.Contains(relativePath, separator+".."+separator) ||
		strings.HasSuffix(relativePath, separator+"..") {
		return "", grpc.Errorf(codes.InvalidArgument, "GetRepoPath: relative path can't contain directory traversal")
	}

	return path.Join(storagePath, relativePath), nil
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

// IsValidRef checks if a ref in a repo is valid
func IsValidRef(ctx context.Context, path, ref string) bool {
	if path == "" || ref == "" {
		return false
	}

	cmd, err := GitCommandReader(ctx, "--git-dir", path, "log", "--max-count=1", ref)
	if err != nil {
		return false
	}
	defer cmd.CleanUpProcessGroup(ctx)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = nil, nil, nil

	return cmd.Wait() == nil
}
