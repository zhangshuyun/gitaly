package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	if err := s.createRepository(
		ctx,
		req.GetRepository(),
		func(repo *gitalypb.Repository) error {
			// We do not want to seed the repository with any contents, so we just
			// return directly.
			return nil
		},
		withBranchName(string(req.GetDefaultBranch()))); err != nil {
		return nil, helper.ErrInternalf("creating repository: %w", err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
