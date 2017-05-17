package helper

import (
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
	var repoPath string

	if storagePath, ok := config.StoragePath(repo.GetStorageName()); ok {
		relativePath := repo.GetRelativePath()
		// Disallow directory traversal for security
		separator := string(os.PathSeparator)
		if strings.HasPrefix(relativePath, ".."+separator) ||
			strings.Contains(relativePath, separator+".."+separator) ||
			strings.HasSuffix(relativePath, separator+"..") {
			return "", grpc.Errorf(codes.InvalidArgument, "GetRepoPath: relative path can't contain directory traversal")
		}
		repoPath = path.Join(storagePath, relativePath)
	} else {
		repoPath = repo.GetPath()
	}

	if repoPath == "" {
		return "", grpc.Errorf(codes.InvalidArgument, "GetRepoPath: empty repo")
	}

	if _, err := os.Stat(path.Join(repoPath, "objects")); err != nil {
		return "", grpc.Errorf(codes.NotFound, "GetRepoPath: not a git repository '%s'", repoPath)
	}

	return repoPath, nil
}
