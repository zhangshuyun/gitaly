package repository

//nolint:depguard
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	worktreePrefix = "gitlab-worktree"
)

func (s *server) Cleanup(ctx context.Context, in *gitalypb.CleanupRequest) (*gitalypb.CleanupResponse, error) {
	repo := s.localrepo(in.GetRepository())

	if err := cleanupWorktrees(ctx, repo); err != nil {
		return nil, err
	}

	return &gitalypb.CleanupResponse{}, nil
}

func cleanupWorktrees(ctx context.Context, repo *localrepo.Repo) error {
	if _, err := repo.Path(); err != nil {
		return err
	}

	worktreeThreshold := time.Now().Add(-6 * time.Hour)
	if err := cleanStaleWorktrees(ctx, repo, worktreeThreshold); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanStaleWorktrees: %v", err)
	}

	if err := cleanDisconnectedWorktrees(ctx, repo); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanDisconnectedWorktrees: %v", err)
	}

	return nil
}

func cleanStaleWorktrees(ctx context.Context, repo *localrepo.Repo, threshold time.Time) error {
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
			err := removeWorktree(ctx, repo, info.Name())
			switch {
			case errors.Is(err, errUnknownWorktree):
				// if git doesn't recognise the worktree then we can safely remove it
				if err := os.RemoveAll(filepath.Join(worktreePath, info.Name())); err != nil {
					return fmt.Errorf("worktree remove dir: %w", err)
				}
			case err != nil:
				return err
			}
		}
	}

	return nil
}

// errUnknownWorktree indicates that git does not recognise the worktree
var errUnknownWorktree = errors.New("unknown worktree")

func removeWorktree(ctx context.Context, repo *localrepo.Repo, name string) error {
	var stderr bytes.Buffer
	err := repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "worktree",
		Action: "remove",
		Flags:  []git.Option{git.Flag{Name: "--force"}},
		Args:   []string{name},
	},
		git.WithRefTxHook(repo),
		git.WithStderr(&stderr),
	)
	if isExitWithCode(err, 128) && strings.HasPrefix(stderr.String(), "fatal: '"+name+"' is not a working tree") {
		return errUnknownWorktree
	} else if err != nil {
		return fmt.Errorf("remove worktree: %w, stderr: %q", err, stderr.String())
	}

	return nil
}

func isExitWithCode(err error, code int) bool {
	actual, ok := command.ExitStatus(err)
	if !ok {
		return false
	}

	return code == actual
}

func cleanDisconnectedWorktrees(ctx context.Context, repo *localrepo.Repo) error {
	return repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "worktree",
		Action: "prune",
	}, git.WithRefTxHook(repo))
}
