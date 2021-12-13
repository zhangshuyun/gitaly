package praefect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// RemoveRepositoryHandler intercepts RemoveRepository calls, deletes the database records and
// deletes the repository from every backing Gitaly node.
func RemoveRepositoryHandler(rs datastore.RepositoryStore, conns Connections) grpc.StreamHandler {
	return func(srv interface{}, stream grpc.ServerStream) error {
		var req gitalypb.RemoveRepositoryRequest
		if err := stream.RecvMsg(&req); err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		ctx := stream.Context()
		repo := req.GetRepository()
		if repo == nil {
			return errMissingRepository
		}

		virtualStorage := repo.StorageName
		replicaPath, storages, err := rs.DeleteRepository(ctx, virtualStorage, repo.RelativePath)
		if err != nil {
			// Gitaly doesn't return an error if the repository is not found, so Praefect follows the
			// same protocol.
			if errors.As(err, new(commonerr.RepositoryNotFoundError)) {
				if featureflag.AtomicRemoveRepository.IsEnabled(ctx) {
					return helper.ErrNotFoundf("repository does not exist")
				}

				return stream.SendMsg(&gitalypb.RemoveRepositoryResponse{})
			}

			return fmt.Errorf("delete repository: %w", err)
		}

		var wg sync.WaitGroup

		storageSet := make(map[string]struct{}, len(storages))
		for _, storage := range storages {
			storageSet[storage] = struct{}{}
		}

		// It's not critical these deletions complete as the background crawler will identify these repos as deleted.
		// To rather return a successful code to the client, we limit the timeout here to 10s.
		ctx, cancel := context.WithTimeout(stream.Context(), 10*time.Second)
		defer cancel()

		for storage, conn := range conns[virtualStorage] {
			if _, ok := storageSet[storage]; !ok {
				// There may be database records for replicas which exist on storages that are not configured in the
				// local Praefect. We'll just ignore them here and not explicitly attempt to delete them. They'll be handled
				// by the background cleaner like any other stale repository if the storages are returned to the configuration.
				continue
			}

			wg.Add(1)
			go func(rewrittenStorage string, conn *grpc.ClientConn) {
				defer wg.Done()

				req := proto.Clone(&req).(*gitalypb.RemoveRepositoryRequest)
				req.Repository.StorageName = rewrittenStorage
				req.Repository.RelativePath = replicaPath

				if _, err := gitalypb.NewRepositoryServiceClient(conn).RemoveRepository(ctx, req); err != nil {
					ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
						"virtual_storage": virtualStorage,
						"relative_path":   repo.RelativePath,
						"storage":         rewrittenStorage,
					}).WithError(err).Error("failed deleting repository")
				}
			}(storage, conn)
		}

		wg.Wait()

		return stream.SendMsg(&gitalypb.RemoveRepositoryResponse{})
	}
}
