package praefect

import (
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// RenameRepositoryHandler handles /gitaly.RepositoryService/RenameRepository calls by renaming
// the repository in the lookup table stored in the database.
func RenameRepositoryHandler(virtualStoragesNames []string, rs datastore.RepositoryStore) grpc.StreamHandler {
	virtualStorages := make(map[string]struct{}, len(virtualStoragesNames))
	for _, virtualStorage := range virtualStoragesNames {
		virtualStorages[virtualStorage] = struct{}{}
	}

	return func(srv interface{}, stream grpc.ServerStream) error {
		var req gitalypb.RenameRepositoryRequest
		if err := stream.RecvMsg(&req); err != nil {
			return fmt.Errorf("receive request: %w", err)
		}

		// These checks are not strictly necessary but they exist to keep retain compatibility with
		// Gitaly's tested behavior.
		if req.GetRepository() == nil {
			return helper.ErrInvalidArgumentf("empty Repository")
		} else if req.GetRelativePath() == "" {
			return helper.ErrInvalidArgumentf("destination relative path is empty")
		} else if _, ok := virtualStorages[req.GetRepository().GetStorageName()]; !ok {
			return helper.ErrInvalidArgumentf("GetStorageByName: no such storage: %q", req.GetRepository().GetStorageName())
		} else if _, err := storage.ValidateRelativePath("/fake-root", req.GetRelativePath()); err != nil {
			// Gitaly uses ValidateRelativePath to verify there are no traversals, so we use the same function
			// here. Praefect is not susceptible to path traversals as it generates its own disk paths but we
			// do this to retain API compatibility with Gitaly. ValidateRelativePath checks for traversals by
			// seeing whether the relative path escapes the root directory. It's not possible to traverse up
			// from the /, so the traversals in the path wouldn't be caught. To allow for the check to work,
			// we use the /fake-root directory simply to notice if there were traversals in the path.
			return helper.ErrInvalidArgumentf("GetRepoPath: %s", err)
		}

		if err := rs.RenameRepositoryInPlace(stream.Context(),
			req.GetRepository().GetStorageName(),
			req.GetRepository().GetRelativePath(),
			req.GetRelativePath(),
		); err != nil {
			if errors.Is(err, commonerr.ErrRepositoryNotFound) {
				return helper.ErrNotFoundf(
					`GetRepoPath: not a git repository: "%s/%s"`,
					req.GetRepository().GetStorageName(),
					req.GetRepository().GetRelativePath(),
				)
			} else if errors.Is(err, commonerr.ErrRepositoryAlreadyExists) {
				return helper.ErrAlreadyExistsf("destination already exists")
			}

			return helper.ErrInternal(err)
		}

		return stream.SendMsg(&gitalypb.RenameRepositoryResponse{})
	}
}
