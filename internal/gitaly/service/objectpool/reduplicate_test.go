package objectpool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestReduplicate(t *testing.T) {
	cfg, repo, repoPath, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	pool, err := objectpool.NewObjectPool(cfg, locator, gitCmdFactory, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()
	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "gc")

	existingObjectID := "55bc176024cfa3baaceb71db584c7e5df900ea65"

	// Corrupt the repository to check if the object can't be found
	altPath, err := locator.InfoAlternatesPath(repo)
	require.NoError(t, err, "find info/alternates")
	require.NoError(t, os.RemoveAll(altPath))

	cmd, err := gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "cat-file", Flags: []git.Option{git.Flag{Name: "-e"}}, Args: []string{existingObjectID}})
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	// Reduplicate and check if the objects appear again
	require.NoError(t, pool.Link(ctx, repo))
	_, err = client.ReduplicateRepository(ctx, &gitalypb.ReduplicateRepositoryRequest{Repository: repo})
	require.NoError(t, err)

	require.NoError(t, pool.Unlink(ctx, repo))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "cat-file", "-e", existingObjectID)
}
