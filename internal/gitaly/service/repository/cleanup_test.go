package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
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
			ctx := testhelper.Context(t)
			repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			req := &gitalypb.CleanupRequest{Repository: repo}

			worktreeCheckoutPath := filepath.Join(repoPath, worktreePrefix, "test-worktree")
			gittest.AddWorktree(t, cfg, repoPath, worktreeCheckoutPath)
			basePath := filepath.Join(repoPath, "worktrees")
			worktreePath := filepath.Join(basePath, "test-worktree")

			require.NoError(t, os.Chtimes(worktreeCheckoutPath, tc.worktreeTime, tc.worktreeTime))

			c, err := client.Cleanup(ctx, req)

			// Sanity check
			assert.FileExists(t, filepath.Join(repoPath, "HEAD")) // For good measure

			if tc.shouldExist {
				assert.DirExists(t, worktreeCheckoutPath)
				assert.DirExists(t, worktreePath)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, c)

				require.NoDirExists(t, worktreeCheckoutPath)
				require.NoDirExists(t, worktreePath)
			}
		})
	}
}

func TestCleanupDeletesOrphanedWorktrees(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(ctx, t)

	worktreeCheckoutPath := filepath.Join(repoPath, worktreePrefix, "test-worktree")
	basePath := filepath.Join(repoPath, "worktrees")
	worktreePath := filepath.Join(basePath, "test-worktree")

	require.NoError(t, os.MkdirAll(worktreeCheckoutPath, os.ModePerm))
	require.NoError(t, os.Chtimes(worktreeCheckoutPath, oldTreeTime, oldTreeTime))

	c, err := client.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	require.NoDirExists(t, worktreeCheckoutPath)
	require.NoDirExists(t, worktreePath)
}

// TODO: replace emulated rebase RPC with actual
// https://gitlab.com/gitlab-org/gitaly/issues/1750
func TestCleanupDisconnectedWorktrees(t *testing.T) {
	t.Parallel()
	const (
		worktreeName     = "test-worktree"
		worktreeAdminDir = "worktrees"
	)

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

	worktreePath := filepath.Join(repoPath, worktreePrefix, worktreeName)
	worktreeAdminPath := filepath.Join(
		repoPath, worktreeAdminDir, filepath.Base(worktreeName),
	)

	req := &gitalypb.CleanupRequest{Repository: repo}

	gittest.AddWorktree(t, cfg, repoPath, worktreePath)

	// removing the work tree path but leaving the administrative files in
	// $GIT_DIR/worktrees will result in the work tree being in a
	// "disconnected" state
	err := os.RemoveAll(worktreePath)
	require.NoError(t, err,
		"disconnecting worktree by removing work tree at %s should succeed", worktreePath,
	)

	cmd := gittest.NewCommand(t, cfg, gittest.AddWorktreeArgs(repoPath, worktreePath)...)
	require.Error(t, cmd.Run(), "creating a new work tree at the same path as a disconnected work tree should fail")

	// cleanup should prune the disconnected worktree administrative files
	_, err = client.Cleanup(ctx, req)
	require.NoError(t, err)
	require.NoDirExists(t, worktreeAdminPath)

	// if the worktree administrative files are pruned, then we should be able
	// to checkout another worktree at the same path
	gittest.AddWorktree(t, cfg, repoPath, worktreePath)
}

func TestCleanupDisconnectedWorktrees_doesNothingWithoutWorktrees(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, _ := setupRepositoryService(ctx, t)
	worktreePath := filepath.Join(testhelper.TempDir(t), "worktree")

	failingGitCmdFactory := gittest.NewInterceptingCommandFactory(ctx, t, cfg, func(git.ExecutionEnvironment) string {
		return `#!/usr/bin/env bash
		exit 15
		`
	})

	repo := localrepo.New(config.NewLocator(cfg), failingGitCmdFactory, nil, repoProto)

	// If this command did spawn git-worktree(1) we'd see an error. It doesn't though because it
	// detects that there aren't any worktrees at all.
	require.NoError(t, cleanDisconnectedWorktrees(ctx, repo))

	gittest.AddWorktree(t, cfg, repoPath, worktreePath)

	// We have now added a worktree now, so it should detect that there are worktrees and thus
	// spawn the Git command. We thus expect the error code we inject via the failing Git
	// command factory.
	require.EqualError(t, cleanDisconnectedWorktrees(ctx, repo), "exit status 15")
}

func TestRemoveWorktree(t *testing.T) {
	t.Parallel()

	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	existingWorktreePath := filepath.Join(repoPath, worktreePrefix, "existing")
	gittest.AddWorktree(t, cfg, repoPath, existingWorktreePath)

	disconnectedWorktreePath := filepath.Join(repoPath, worktreePrefix, "disconnected")
	gittest.AddWorktree(t, cfg, repoPath, disconnectedWorktreePath)
	require.NoError(t, os.RemoveAll(disconnectedWorktreePath))

	orphanedWorktreePath := filepath.Join(repoPath, worktreePrefix, "orphaned")
	require.NoError(t, os.MkdirAll(orphanedWorktreePath, os.ModePerm))

	for _, tc := range []struct {
		worktree     string
		errorIs      error
		expectExists bool
	}{
		{
			worktree:     "existing",
			expectExists: false,
		},
		{
			worktree:     "disconnected",
			expectExists: false,
		},
		{
			worktree:     "unknown",
			errorIs:      errUnknownWorktree,
			expectExists: false,
		},
		{
			worktree:     "orphaned",
			errorIs:      errUnknownWorktree,
			expectExists: true,
		},
	} {
		t.Run(tc.worktree, func(t *testing.T) {
			ctx := testhelper.Context(t)

			worktreePath := filepath.Join(repoPath, worktreePrefix, tc.worktree)

			err := removeWorktree(ctx, repo, tc.worktree)
			if tc.errorIs == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.errorIs)
			}

			if tc.expectExists {
				require.DirExists(t, worktreePath)
			} else {
				require.NoDirExists(t, worktreePath)
			}
		})
	}
}
