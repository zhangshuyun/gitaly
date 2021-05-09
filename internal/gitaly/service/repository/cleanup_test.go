package repository

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestCleanupDeletesRefsLocks(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.CleanupRequest{Repository: repo}
	refsPath := filepath.Join(repoPath, "refs")

	keepRefPath := filepath.Join(refsPath, "heads", "keepthis")
	mustCreateFileWithTimes(t, keepRefPath, freshTime)
	keepOldRefPath := filepath.Join(refsPath, "heads", "keepthisalso")
	mustCreateFileWithTimes(t, keepOldRefPath, oldTime)
	keepDeceitfulRef := filepath.Join(refsPath, "heads", " .lock.not-actually-a-lock.lock ")
	mustCreateFileWithTimes(t, keepDeceitfulRef, oldTime)

	keepLockPath := filepath.Join(refsPath, "heads", "keepthis.lock")
	mustCreateFileWithTimes(t, keepLockPath, freshTime)

	deleteLockPath := filepath.Join(refsPath, "heads", "deletethis.lock")
	mustCreateFileWithTimes(t, deleteLockPath, oldTime)

	c, err := client.Cleanup(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Sanity checks
	assert.FileExists(t, keepRefPath)
	assert.FileExists(t, keepOldRefPath)
	assert.FileExists(t, keepDeceitfulRef)

	assert.FileExists(t, keepLockPath)

	require.NoFileExists(t, deleteLockPath)
}

func TestCleanupDeletesPackedRefsLock(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc        string
		lockTime    *time.Time
		shouldExist bool
	}{
		{
			desc:        "with a recent lock",
			lockTime:    &freshTime,
			shouldExist: true,
		},
		{
			desc:        "with an old lock",
			lockTime:    &oldTime,
			shouldExist: false,
		},
		{
			desc:        "with a non-existing lock",
			lockTime:    nil,
			shouldExist: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			t.Cleanup(cleanupFn)

			// Force the packed-refs file to have an old time to test that even
			// in that case it doesn't get deleted
			packedRefsPath := filepath.Join(repoPath, "packed-refs")
			require.NoError(t, os.Chtimes(packedRefsPath, oldTime, oldTime))

			req := &gitalypb.CleanupRequest{Repository: repo}
			lockPath := filepath.Join(repoPath, "packed-refs.lock")

			if tc.lockTime != nil {
				mustCreateFileWithTimes(t, lockPath, *tc.lockTime)
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.Cleanup(ctx, req)

			// Sanity checks
			assert.FileExists(t, filepath.Join(repoPath, "HEAD")) // For good measure
			assert.FileExists(t, packedRefsPath)

			if tc.shouldExist {
				assert.FileExists(t, lockPath)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, c)

				require.NoFileExists(t, lockPath)
			}
		})
	}
}

// TODO: replace emulated rebase RPC with actual
// https://gitlab.com/gitlab-org/gitaly/issues/1750
func TestCleanupDeletesStaleWorktrees(t *testing.T) {
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

func TestCleanupFileLocks(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.CleanupRequest{Repository: repo}

	for _, fileName := range []string{
		"config.lock",
		"HEAD.lock",
		"objects/info/commit-graphs/commit-graph-chain.lock",
	} {
		lockPath := filepath.Join(repoPath, fileName)
		// No file on the lock path
		_, err := client.Cleanup(ctx, req)
		assert.NoError(t, err)

		// Fresh lock should remain
		mustCreateFileWithTimes(t, lockPath, freshTime)
		_, err = client.Cleanup(ctx, req)
		assert.NoError(t, err)
		assert.FileExists(t, lockPath)

		// Old lock should be removed
		mustCreateFileWithTimes(t, lockPath, oldTime)
		_, err = client.Cleanup(ctx, req)
		assert.NoError(t, err)
		require.NoFileExists(t, lockPath)
	}
}

func TestCleanupDeletesPackedRefsNew(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc        string
		lockTime    *time.Time
		shouldExist bool
	}{
		{
			desc:        "created recently",
			lockTime:    &freshTime,
			shouldExist: true,
		},
		{
			desc:        "exists for too long",
			lockTime:    &oldTime,
			shouldExist: false,
		},
		{
			desc:        "nothing to clean up",
			shouldExist: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			t.Cleanup(cleanupFn)

			req := &gitalypb.CleanupRequest{Repository: repo}
			packedRefsNewPath := filepath.Join(repoPath, "packed-refs.new")

			if tc.lockTime != nil {
				mustCreateFileWithTimes(t, packedRefsNewPath, *tc.lockTime)
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.Cleanup(ctx, req)
			require.NotNil(t, c)
			require.NoError(t, err)

			if tc.shouldExist {
				require.FileExists(t, packedRefsNewPath)
			} else {
				require.NoFileExists(t, packedRefsNewPath)
			}
		})
	}
}
