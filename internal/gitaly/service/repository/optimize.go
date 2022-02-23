package repository

import (
	"context"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	repo := s.localrepo(in.GetRepository())

	if err := s.housekeepingManager.OptimizeRepository(ctx, repo); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func (s *server) validateOptimizeRepositoryRequest(in *gitalypb.OptimizeRepositoryRequest) error {
	if in.GetRepository() == nil {
		return helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}

	_, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	return nil
}
