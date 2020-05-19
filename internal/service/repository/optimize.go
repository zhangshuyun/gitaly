package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	optimizeEmptyDirRemovalTotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "repository",
			Name:      "optimizerepository_empty_dir_removal_total",
			Help:      "Total number of empty directories removed by OptimizeRepository RPC",
		},
	)
)

func init() {
	prometheus.MustRegister(optimizeEmptyDirRemovalTotals)
}

func removeEmptyDirs(ctx context.Context, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := ioutil.ReadDir(target)
	switch {
	case os.IsNotExist(err):
		return nil // race condition: someone else deleted it first
	case err != nil:
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		ePath := filepath.Join(target, e.Name())
		if err := removeEmptyDirs(ctx, ePath); err != nil {
			return err
		}
	}

	// recheck entries now that we have potentially removed some dirs
	entries, err = ioutil.ReadDir(target)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if len(entries) > 0 {
		return nil
	}

	switch err := os.Remove(target); {
	case os.IsNotExist(err):
		return nil // race condition: someone else deleted it first
	case err != nil:
		return err
	}
	optimizeEmptyDirRemovalTotals.Inc()

	return nil
}

func removeRefEmptyDirs(ctx context.Context, repository *gitalypb.Repository) error {
	rPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}
	repoRefsPath := filepath.Join(rPath, "refs")

	// we never want to delete the actual "refs" directory, so we start the
	// recursive functions for each subdirectory
	entries, err := ioutil.ReadDir(repoRefsPath)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		ePath := filepath.Join(repoRefsPath, e.Name())
		if err := removeEmptyDirs(ctx, ePath); err != nil {
			return err
		}
	}

	return nil
}

func (s *server) optimizeRepository(ctx context.Context, repository *gitalypb.Repository) error {
	hasBitmap, err := stats.HasBitmap(repository)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if !hasBitmap {
		altFile, err := git.InfoAlternatesPath(repository)
		if err != nil {
			return helper.ErrInternal(err)
		}

		// repositories with alternates should never have a bitmap, as Git will otherwise complain about
		// multiple bitmaps being present in parent and alternate repository.
		if _, err = os.Stat(altFile); !os.IsNotExist(err) {
			return nil
		}

		_, err = s.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repository, CreateBitmap: true})
		if err != nil {
			return err
		}
	}

	if err := removeRefEmptyDirs(ctx, repository); err != nil {
		return fmt.Errorf("OptimizeRepository: remove empty refs: %w", err)
	}

	return nil
}

func (s *server) OptimizeRepository(ctx context.Context, in *gitalypb.OptimizeRepositoryRequest) (*gitalypb.OptimizeRepositoryResponse, error) {
	if err := validateOptimizeRepositoryRequest(in); err != nil {
		return nil, err
	}

	if err := s.optimizeRepository(ctx, in.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func validateOptimizeRepositoryRequest(in *gitalypb.OptimizeRepositoryRequest) error {
	if in.GetRepository() == nil {
		return helper.ErrInvalidArgumentf("empty repository")
	}

	_, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	return nil
}
