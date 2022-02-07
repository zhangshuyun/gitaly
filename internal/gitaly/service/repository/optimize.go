package repository

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := s.validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	repo := s.localrepo(in.GetRepository())

	if err := s.optimizeRepository(ctx, repo); err != nil {
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

func (s *server) optimizeRepository(ctx context.Context, repo *localrepo.Repo) error {
	if err := housekeeping.Perform(ctx, repo, s.txManager); err != nil {
		return fmt.Errorf("could not execute houskeeping: %w", err)
	}

	if err := repackIfNeeded(ctx, repo); err != nil {
		return fmt.Errorf("could not repack: %w", err)
	}

	return nil
}

// repackIfNeeded uses a set of heuristics to determine whether the repository needs a
// full repack and, if so, repacks it.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo) error {
	repackNeeded, cfg, err := needsRepacking(repo)
	if err != nil {
		return fmt.Errorf("determining whether repo needs repack: %w", err)
	}

	if !repackNeeded {
		return nil
	}

	if err := repack(ctx, repo, cfg); err != nil {
		return err
	}

	return nil
}

func needsRepacking(repo *localrepo.Repo) (bool, repackCommandConfig, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return false, repackCommandConfig{}, fmt.Errorf("getting repository path: %w", err)
	}

	altFile, err := repo.InfoAlternatesPath()
	if err != nil {
		return false, repackCommandConfig{}, helper.ErrInternal(err)
	}

	hasAlternate := true
	if _, err := os.Stat(altFile); os.IsNotExist(err) {
		hasAlternate = false
	}

	hasBitmap, err := stats.HasBitmap(repoPath)
	if err != nil {
		return false, repackCommandConfig{}, fmt.Errorf("checking for bitmap: %w", err)
	}

	// Bitmaps are used to efficiently determine transitive reachability of objects from a
	// set of commits. They are an essential part of the puzzle required to serve fetches
	// efficiently, as we'd otherwise need to traverse the object graph every time to find
	// which objects we have to send. We thus repack the repository with bitmaps enabled in
	// case they're missing.
	//
	// There is one exception: repositories which are connected to an object pool must not have
	// a bitmap on their own. We do not yet use multi-pack indices, and in that case Git can
	// only use one bitmap. We already generate this bitmap in the pool, so member of it
	// shouldn't have another bitmap on their own.
	if !hasBitmap && !hasAlternate {
		return true, repackCommandConfig{
			fullRepack:  true,
			writeBitmap: true,
		}, nil
	}

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	if err != nil {
		return false, repackCommandConfig{}, fmt.Errorf("checking for bloom filters: %w", err)
	}

	// Bloom filters are part of the commit-graph and allow us to efficiently determine which
	// paths have been modified in a given commit without having to look into the object
	// database. In the past we didn't compute bloom filters at all, so we want to rewrite the
	// whole commit-graph to generate them.
	//
	// Note that we'll eventually want to move out commit-graph generation from repacking. When
	// that happens we should update the commit-graph either if it's missing, when bloom filters
	// are missing or when packfiles have been updated.
	if missingBloomFilters {
		return true, repackCommandConfig{
			fullRepack:  true,
			writeBitmap: !hasAlternate,
		}, nil
	}

	return false, repackCommandConfig{}, nil
}
