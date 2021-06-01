package praefect

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errMissingRepository   = status.Error(codes.InvalidArgument, "missing repository")
	errMissingStorageName  = status.Error(codes.InvalidArgument, "repository missing storage name")
	errMissingRelativePath = status.Error(codes.InvalidArgument, "repository missing relative path")
)

// RepositoryExistsStreamInterceptor returns a stream interceptor that handles /gitaly.RepositoryService/RepositoryExists
// calls by checking whether there is a record of the repository in the database.
func RepositoryExistsStreamInterceptor(rs datastore.RepositoryStore) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod != "/gitaly.RepositoryService/RepositoryExists" {
			return handler(srv, stream)
		}

		var req gitalypb.RepositoryExistsRequest
		if err := stream.RecvMsg(&req); err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		repo := req.GetRepository()
		if repo == nil {
			return errMissingRepository
		}

		storageName := repo.StorageName
		if storageName == "" {
			return errMissingStorageName
		}

		relativePath := repo.RelativePath
		if relativePath == "" {
			return errMissingRelativePath
		}

		exists, err := rs.RepositoryExists(stream.Context(), storageName, relativePath)
		if err != nil {
			return fmt.Errorf("repository exists: %w", err)
		}

		if err := stream.SendMsg(&gitalypb.RepositoryExistsResponse{Exists: exists}); err != nil {
			return fmt.Errorf("send response: %w", err)
		}

		return nil
	}
}
