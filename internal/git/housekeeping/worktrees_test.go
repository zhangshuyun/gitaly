package housekeeping

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestCleanupDisconnectedWorktrees_doesNothingWithoutWorktrees(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
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
