package repository

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) RemoveRepository(ctx context.Context, in *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	repo := in.GetRepository()

	path, err := s.locator.GetPath(repo)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	storage, ok := s.cfg.Storage(repo.GetStorageName())
	if !ok {
		return nil, helper.ErrInvalidArgumentf("storage %v not found", repo.GetStorageName())
	}

	tempDir := tempdir.TempDir(storage)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, helper.ErrInternal(err)
	}

	base := filepath.Base(path)
	destDir := filepath.Join(tempDir, base+"+removed")

	if err := os.Rename(path, destDir); err != nil {
		if os.IsNotExist(err) {
			return &gitalypb.RemoveRepositoryResponse{}, nil
		}
		return nil, helper.ErrInternal(err)
	}

	if err := os.RemoveAll(destDir); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.RemoveRepositoryResponse{}, nil
}
