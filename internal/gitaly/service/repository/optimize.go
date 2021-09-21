package repository

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// repackIfNeeded uses a set of heuristics to determine whether the repository needs a
// full repack and, if so, repacks it.
func (s *server) repackIfNeeded(ctx context.Context, repository *gitalypb.Repository) error {
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	hasBitmap, err := stats.HasBitmap(repoPath)
	if err != nil {
		return helper.ErrInternal(err)
	}

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if hasBitmap && !missingBloomFilters {
		return nil
	}

	altFile, err := s.locator.InfoAlternatesPath(repository)
	if err != nil {
		return helper.ErrInternal(err)
	}

	// Repositories with alternates should never have a bitmap, as Git will otherwise complain about
	// multiple bitmaps being present in parent and alternate repository.
	// In case of an error it still tries it is best to optimise the repository.
	createBitMap := false
	if _, err := os.Stat(altFile); os.IsNotExist(err) {
		createBitMap = true
	}

	if _, err = s.RepackFull(ctx, &gitalypb.RepackFullRequest{
		Repository:   repository,
		CreateBitmap: createBitMap,
	}); err != nil {
		return err
	}

	return nil
}

func (s *server) optimizeRepository(ctx context.Context, repository *gitalypb.Repository) error {
	if err := s.repackIfNeeded(ctx, repository); err != nil {
		return fmt.Errorf("could not repack: %w", err)
	}

	repo := s.localrepo(repository)

	if err := housekeeping.Perform(ctx, repo, s.txManager); err != nil {
		return fmt.Errorf("could not execute houskeeping: %w", err)
	}

	return nil
}

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	if err := s.optimizeRepository(ctx, in.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func (s *server) validateOptimizeRepositoryRequest(in *gitalypb.OptimizeRepositoryRequest) error {
	if in.GetRepository() == nil {
		return helper.ErrInvalidArgumentf("empty repository")
	}

	_, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	return nil
}
