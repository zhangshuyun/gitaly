package repository

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	emptyRefsGracePeriod = 24 * time.Hour
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

	// We need to stat the directory early on in order to get its current mtime. If we
	// did this after we have removed empty child directories, then its mtime would've
	// changed and we wouldn't consider it for deletion.
	dirStat, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
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

	// If the directory is older than the grace period for empty refs, then we can
	// consider it for deletion in case it's empty.
	if time.Since(dirStat.ModTime()) < emptyRefsGracePeriod {
		return nil
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

func (s *server) removeRefEmptyDirs(ctx context.Context, repository *localrepo.Repo) error {
	rPath, err := repository.Path()
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

// repackIfNoBitmap uses the bitmap index as a heuristic to determine whether the repository needs a
// full repack. So only if there is none will the full repack be started.
func (s *server) repackIfNoBitmap(ctx context.Context, repository *gitalypb.Repository) error {
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	hasBitmap, err := stats.HasBitmap(repoPath)
	if err != nil {
		return helper.ErrInternal(err)
	}
	if hasBitmap {
		return nil
	}

	altFile, err := s.locator.InfoAlternatesPath(repository)
	if err != nil {
		return helper.ErrInternal(err)
	}

	// repositories with alternates should never have a bitmap, as Git will otherwise complain about
	// multiple bitmaps being present in parent and alternate repository.
	if _, err = os.Stat(altFile); !os.IsNotExist(err) {
		return nil
	}

	if _, err = s.RepackFull(ctx, &gitalypb.RepackFullRequest{
		Repository:   repository,
		CreateBitmap: true,
	}); err != nil {
		return err
	}

	return nil
}

func (s *server) optimizeRepository(ctx context.Context, repository *gitalypb.Repository) error {
	if err := s.repackIfNoBitmap(ctx, repository); err != nil {
		return fmt.Errorf("could not repack: %w", err)
	}

	repo := localrepo.New(s.gitCmdFactory, repository, s.cfg)

	if err := s.removeRefEmptyDirs(ctx, repo); err != nil {
		return fmt.Errorf("OptimizeRepository: remove empty refs: %w", err)
	}

	// TODO: https://gitlab.com/gitlab-org/gitaly/-/issues/3138
	// This is a temporary code and needs to be removed once it will be run on all repositories at least once.
	if err := s.unsetAllConfigsByRegexp(ctx, repo, "^http\\..+\\.extraHeader$"); err != nil {
		return fmt.Errorf("OptimizeRepository: unset all configs by regexp: %w", err)
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

func (s *server) unsetAllConfigsByRegexp(ctx context.Context, repository *localrepo.Repo, regexp string) error {
	keys, err := s.getConfigKeys(ctx, repository, regexp)
	if err != nil {
		return fmt.Errorf("get config keys: %w", err)
	}

	if err := s.unsetConfigKeys(ctx, repository, keys); err != nil {
		return fmt.Errorf("unset all keys: %w", err)
	}

	return nil
}

func (s *server) getConfigKeys(ctx context.Context, repository *localrepo.Repo, regexp string) ([]string, error) {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name: "config",
		Flags: []git.Option{
			git.Flag{Name: "--name-only"},
			git.ValueFlag{Name: "--get-regexp", Value: regexp},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creation of 'git config': %w", err)
	}

	keys, err := parseConfigKeys(cmd)
	if err != nil {
		return nil, fmt.Errorf("parse config keys: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		var termErr *exec.ExitError
		if errors.As(err, &termErr) {
			if termErr.ExitCode() == 1 {
				// https://git-scm.com/docs/git-config#_description: The section or key is invalid (ret=1)
				// This means no matching values were found.
				return nil, nil
			}
		}
		return nil, fmt.Errorf("wait for 'git config': %w", err)
	}

	return keys, nil
}

func parseConfigKeys(reader io.Reader) ([]string, error) {
	var keys []string

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		keys = append(keys, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return keys, nil
}

func (s *server) unsetConfigKeys(ctx context.Context, repository *localrepo.Repo, names []string) error {
	for _, name := range names {
		if err := s.unsetAll(ctx, repository, name); err != nil {
			return fmt.Errorf("unset all: %w", err)
		}
	}

	return nil
}

func (s *server) unsetAll(ctx context.Context, repository *localrepo.Repo, name string) error {
	if strings.TrimSpace(name) == "" {
		return nil
	}

	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name:  "config",
		Flags: []git.Option{git.ValueFlag{Name: "--unset-all", Value: name}},
	})
	if err != nil {
		return fmt.Errorf("creation of 'git config': %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'git config': %w", err)
	}

	return nil
}
