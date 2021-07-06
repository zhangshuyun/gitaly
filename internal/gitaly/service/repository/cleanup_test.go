package repository

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// TODO: replace emulated rebase RPC with actual
// https://gitlab.com/gitlab-org/gitaly/issues/1750
func TestCleanupDeletesStaleWorktrees(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc         string
		worktreeTime time.Time
		shouldExist  bool
	}{
		{
			desc:         "with a recent worktree",
			worktreeTime: freshTime,
			shouldExist:  true,
		},
		{
			desc:         "with a slightly old worktree",
			worktreeTime: oldTime,
			shouldExist:  true,
		},
		{
			desc:         "with an old worktree",
			worktreeTime: oldTreeTime,
			shouldExist:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			t.Cleanup(cleanupFn)

			req := &gitalypb.CleanupRequest{Repository: repo}

			worktreeCheckoutPath := filepath.Join(repoPath, worktreePrefix, "test-worktree")
			gittest.AddWorktree(t, cfg, repoPath, worktreeCheckoutPath)
			basePath := filepath.Join(repoPath, "worktrees")
			worktreePath := filepath.Join(basePath, "test-worktree")

			require.NoError(t, os.Chtimes(worktreeCheckoutPath, tc.worktreeTime, tc.worktreeTime))

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.Cleanup(ctx, req)

			// Sanity check
			assert.FileExists(t, filepath.Join(repoPath, "HEAD")) // For good measure

			if tc.shouldExist {
				assert.DirExists(t, worktreeCheckoutPath)
				assert.DirExists(t, worktreePath)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, c)

				require.NoFileExists(t, worktreeCheckoutPath)
				require.NoFileExists(t, worktreePath)
			}
		})
	}
}

// TODO: replace emulated rebase RPC with actual
// https://gitlab.com/gitlab-org/gitaly/issues/1750
func TestCleanupDisconnectedWorktrees(t *testing.T) {
	t.Parallel()
	const (
		worktreeName     = "test-worktree"
		worktreeAdminDir = "worktrees"
	)

	cfg, repo, repoPath, client := setupRepositoryService(t)

	worktreePath := filepath.Join(repoPath, worktreePrefix, worktreeName)
	worktreeAdminPath := filepath.Join(
		repoPath, worktreeAdminDir, filepath.Base(worktreeName),
	)

	req := &gitalypb.CleanupRequest{Repository: repo}

	gittest.AddWorktree(t, cfg, repoPath, worktreePath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// removing the work tree path but leaving the administrative files in
	// $GIT_DIR/worktrees will result in the work tree being in a
	// "disconnected" state
	err := os.RemoveAll(worktreePath)
	require.NoError(t, err,
		"disconnecting worktree by removing work tree at %s should succeed", worktreePath,
	)

	err = exec.Command(cfg.Git.BinPath, gittest.AddWorktreeArgs(repoPath, worktreePath)...).Run()
	require.Error(t, err, "creating a new work tree at the same path as a disconnected work tree should fail")

	// cleanup should prune the disconnected worktree administrative files
	_, err = client.Cleanup(ctx, req)
	require.NoError(t, err)
	require.NoFileExists(t, worktreeAdminPath)

	// if the worktree administrative files are pruned, then we should be able
	// to checkout another worktree at the same path
	gittest.AddWorktree(t, cfg, repoPath, worktreePath)
}
