package repository

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestPruneUnreachableObjects(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	setObjectTime := func(t *testing.T, repoPath string, objectID git.ObjectID, when time.Time) {
		looseObjectPath := filepath.Join(repoPath, "objects", objectID.String()[:2], objectID.String()[2:])
		require.NoError(t, os.Chtimes(looseObjectPath, when, when))
	}

	t.Run("missing repository", func(t *testing.T) {
		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{})
		if testhelper.IsPraefectEnabled() {
			testhelper.RequireGrpcError(t, helper.ErrInvalidArgumentf("repo scoped: empty Repository"), err)
		} else {
			testhelper.RequireGrpcError(t, helper.ErrInvalidArgumentf("missing repository"), err)
		}
	})

	t.Run("relative path points to removed repository", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(ctx, t, cfg)
		require.NoError(t, os.RemoveAll(repoPath))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		testhelper.RequireGrpcError(t, helper.ErrNotFoundf("GetRepoPath: not a git repository: %q", repoPath), err)
	})

	t.Run("empty repository", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(ctx, t, cfg)

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
	})

	t.Run("repository with reachable objects", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(ctx, t, cfg)

		// Create the commit and a branch pointing to it to make it reachable.
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(), gittest.WithBranch("branch"))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// Verify we can still read the commit.
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
	})

	t.Run("repository with recent unreachable objects", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(ctx, t, cfg)

		// Create the commit, but don't create a reference pointing to it.
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		// Set the object time to something that's close to 30 minutes, but gives us enough
		// room to not cause flakes.
		setObjectTime(t, repoPath, commitID, time.Now().Add(-28*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// Verify we can still read the commit. The commit isn't older than 30 minutes, so
		// it shouldn't be pruned.
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
	})

	t.Run("repository with old unreachable objects", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(ctx, t, cfg)

		// Create the commit, but don't create a reference pointing to it.
		commitID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		setObjectTime(t, repoPath, commitID, time.Now().Add(-31*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		cmd := gittest.NewCommand(t, cfg, "-C", repoPath, "rev-parse", "--verify", commitID.String()+"^{commit}")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Equal(t, "fatal: Needed a single revision\n", string(output))
	})

	t.Run("repository with mixed objects", func(t *testing.T) {
		repo, repoPath := gittest.CreateRepository(ctx, t, cfg)

		reachableOldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(), gittest.WithMessage("a"), gittest.WithBranch("branch"))
		setObjectTime(t, repoPath, reachableOldCommit, time.Now().Add(-31*time.Minute))

		unreachableRecentCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("b"), gittest.WithParents())
		setObjectTime(t, repoPath, unreachableRecentCommit, time.Now().Add(-28*time.Minute))

		unreachableOldCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("c"), gittest.WithParents())
		setObjectTime(t, repoPath, unreachableOldCommit, time.Now().Add(-31*time.Minute))

		_, err := client.PruneUnreachableObjects(ctx, &gitalypb.PruneUnreachableObjectsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		// The reachable old and unreachable recent commits should still exist.
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", reachableOldCommit.String()+"^{commit}")
		gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "--verify", unreachableRecentCommit.String()+"^{commit}")

		// But the unreachable old commit should have been pruned.
		cmd := gittest.NewCommand(t, cfg, "-C", repoPath, "rev-parse", "--verify", unreachableOldCommit.String()+"^{commit}")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Equal(t, "fatal: Needed a single revision\n", string(output))
	})
}
