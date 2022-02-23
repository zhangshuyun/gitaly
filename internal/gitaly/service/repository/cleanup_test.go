package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	worktreePrefix = "gitlab-worktree"
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

			//nolint:staticcheck
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

	//nolint:staticcheck
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
	//nolint:staticcheck
	_, err = client.Cleanup(ctx, req)
	require.NoError(t, err)
	require.NoDirExists(t, worktreeAdminPath)

	// if the worktree administrative files are pruned, then we should be able
	// to checkout another worktree at the same path
	gittest.AddWorktree(t, cfg, repoPath, worktreePath)
}

func TestCleanup_invalidRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc string
		in   *gitalypb.Repository
		err  error
	}{
		{
			desc: "no repository provided",
			err:  status.Error(codes.InvalidArgument, gitalyOrPraefect("empty Repository", "repo scoped: empty Repository")),
		},
		{
			desc: "storage doesn't exist",
			in:   &gitalypb.Repository{StorageName: "stub"},
			err:  status.Error(codes.InvalidArgument, gitalyOrPraefect(`GetStorageByName: no such storage: "stub"`, "repo scoped: invalid Repository")),
		},
		{
			desc: "relative path doesn't exist",
			in:   &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "so/me/some.git"},
			err: status.Error(
				codes.NotFound,
				gitalyOrPraefect(
					fmt.Sprintf(`GetRepoPath: not a git repository: %q`, filepath.Join(cfg.Storages[0].Path, "so/me/some.git")),
					`mutator call: route repository mutator: get repository id: repository "default"/"so/me/some.git" not found`,
				),
			),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: tc.in})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}
