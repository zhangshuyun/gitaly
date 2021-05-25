package internalgitaly

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WalkRepos(req *gitalypb.WalkReposRequest, stream gitalypb.InternalGitaly_WalkReposServer) error {
	sPath, err := s.storagePath(req.GetStorageName())
	if err != nil {
		return err
	}

	return walkStorage(stream.Context(), sPath, stream)
}

func (s *server) storagePath(storageName string) (string, error) {
	for _, storage := range s.storages {
		if storage.Name == storageName {
			return storage.Path, nil
		}
	}
	return "", status.Errorf(
		codes.NotFound,
		"storage name %q not found", storageName,
	)
}

func walkStorage(ctx context.Context, storagePath string, stream gitalypb.InternalGitaly_WalkReposServer) error {
	return filepath.Walk(storagePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// keep walking
		}

		if storage.IsGitDirectory(path) {
			relPath, err := filepath.Rel(storagePath, path)
			if err != nil {
				return err
			}

			if err := stream.Send(&gitalypb.WalkReposResponse{
				RelativePath: relPath,
			}); err != nil {
				return err
			}

			// once we know we are inside a git directory, we don't
			// want to continue walking inside since that is
			// resource intensive and unnecessary
			return filepath.SkipDir
		}

		return nil
	})
}
