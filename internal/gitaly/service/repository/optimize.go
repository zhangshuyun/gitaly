package repository

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
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
	optimizations := struct {
		PackedObjects bool `json:"packed_objects"`
	}{}
	defer func() {
		ctxlogrus.Extract(ctx).WithField("optimizations", optimizations).Info("optimized repository")
	}()

	if err := housekeeping.Perform(ctx, repo, s.txManager); err != nil {
		return fmt.Errorf("could not execute houskeeping: %w", err)
	}

	didRepack, err := repackIfNeeded(ctx, repo)
	if err != nil {
		return fmt.Errorf("could not repack: %w", err)
	}
	optimizations.PackedObjects = didRepack

	return nil
}

// repackIfNeeded uses a set of heuristics to determine whether the repository needs a
// full repack and, if so, repacks it.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo) (bool, error) {
	repackNeeded, cfg, err := needsRepacking(repo)
	if err != nil {
		return false, fmt.Errorf("determining whether repo needs repack: %w", err)
	}

	if !repackNeeded {
		return false, nil
	}

	if err := repack(ctx, repo, cfg); err != nil {
		return false, err
	}

	return true, nil
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

	largestPackfileSize, packfileCount, err := packfileSizeAndCount(repo)
	if err != nil {
		return false, repackCommandConfig{}, fmt.Errorf("checking largest packfile size: %w", err)
	}

	// Whenever we do an incremental repack we create a new packfile, and as a result Git may
	// have to look into every one of the packfiles to find objects. This is less efficient the
	// more packfiles we have, but we cannot repack the whole repository every time either given
	// that this may take a lot of time.
	//
	// Instead, we determine whether the repository has "too many" packfiles. "Too many" is
	// relative though: for small repositories it's fine to do full repacks regularly, but for
	// large repositories we need to be more careful. We thus use a heuristic of "repository
	// largeness": we take the biggest packfile that exists, and then the maximum allowed number
	// of packfiles is `log(largestpackfile_size_in_mb) / log(1.3)`. This gives the following
	// allowed number of packfiles:
	//
	// - No packfile: 5 packfile. This is a special case.
	// - 10MB packfile: 8 packfiles.
	// - 100MB packfile: 17 packfiles.
	// - 500MB packfile: 23 packfiles.
	// - 1GB packfile: 26 packfiles.
	// - 5GB packfile: 32 packfiles.
	// - 10GB packfile: 35 packfiles.
	// - 100GB packfile: 43 packfiles.
	//
	// The goal is to have a comparatively quick ramp-up of allowed packfiles as the repository
	// size grows, but then slow down such that we're effectively capped and don't end up with
	// an excessive amount of packfiles.
	//
	// This is a heuristic and thus imperfect by necessity. We may tune it as we gain experience
	// with the way it behaves.
	if int64(math.Max(5, math.Log(float64(largestPackfileSize))/math.Log(1.3))) < packfileCount {
		return true, repackCommandConfig{
			fullRepack:  true,
			writeBitmap: !hasAlternate,
		}, nil
	}

	return false, repackCommandConfig{}, nil
}

func packfileSizeAndCount(repo *localrepo.Repo) (int64, int64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, 0, fmt.Errorf("getting repository path: %w", err)
	}

	entries, err := os.ReadDir(filepath.Join(repoPath, "objects/pack"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil
		}

		return 0, 0, err
	}

	largestSize := int64(0)
	count := int64(0)

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}

		entryInfo, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return 0, 0, fmt.Errorf("getting packfile info: %w", err)
		}

		if entryInfo.Size() > largestSize {
			largestSize = entryInfo.Size()
		}

		count++
	}

	return largestSize / 1024 / 1024, count, nil
}
