package repository

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) Cleanup(ctx context.Context, in *gitalypb.CleanupRequest) (*gitalypb.CleanupResponse, error) {
	repo := s.localrepo(in.GetRepository())

	if err := s.cleanupRepo(ctx, repo); err != nil {
		return nil, err
	}

	return &gitalypb.CleanupResponse{}, nil
}

func (s *server) cleanupRepo(ctx context.Context, repo *localrepo.Repo) error {
	if _, err := repo.Path(); err != nil {
		return err
	}

	worktreeThreshold := time.Now().Add(-6 * time.Hour)
	if err := s.cleanStaleWorktrees(ctx, repo, worktreeThreshold); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanStaleWorktrees: %v", err)
	}

	if err := s.cleanDisconnectedWorktrees(ctx, repo); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanDisconnectedWorktrees: %v", err)
	}

	if err := housekeeping.Perform(ctx, repo); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: houskeeping: %v", err)
	}

	return nil
}

func (s *server) cleanStaleWorktrees(ctx context.Context, repo *localrepo.Repo, threshold time.Time) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	worktreePath := filepath.Join(repoPath, worktreePrefix)

	dirInfo, err := os.Stat(worktreePath)
	if err != nil {
		if os.IsNotExist(err) || !dirInfo.IsDir() {
			return nil
		}
		return err
	}

	worktreeEntries, err := ioutil.ReadDir(worktreePath)
	if err != nil {
		return err
	}

	for _, info := range worktreeEntries {
		if !info.IsDir() || (info.Mode()&os.ModeSymlink != 0) {
			continue
		}

		if info.ModTime().Before(threshold) {
			if err := repo.ExecAndWait(ctx, git.SubSubCmd{
				Name:   "worktree",
				Action: "remove",
				Flags:  []git.Option{git.Flag{Name: "--force"}},
				Args:   []string{info.Name()},
			}, git.WithRefTxHook(ctx, repo, s.cfg)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *server) cleanDisconnectedWorktrees(ctx context.Context, repo *localrepo.Repo) error {
	return repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "worktree",
		Action: "prune",
	}, git.WithRefTxHook(ctx, repo, s.cfg))
}
