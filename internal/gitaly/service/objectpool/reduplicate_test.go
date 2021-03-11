package objectpool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestReduplicate(t *testing.T) {
	locator := config.NewLocator(config.Config)
	server, serverSocketPath := runObjectPoolServer(t, config.Config, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	pool, err := objectpool.NewObjectPool(config.Config, locator, gitCmdFactory, testRepo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)
	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "gc")

	existingObjectID := "55bc176024cfa3baaceb71db584c7e5df900ea65"

	// Corrupt the repository to check if the object can't be found
	altPath, err := locator.InfoAlternatesPath(testRepo)
	require.NoError(t, err, "find info/alternates")
	require.NoError(t, os.RemoveAll(altPath))

	cmd, err := gitCmdFactory.New(ctx, testRepo,
		git.SubCmd{Name: "cat-file", Flags: []git.Option{git.Flag{Name: "-e"}}, Args: []string{existingObjectID}})
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	// Reduplicate and check if the objects appear again
	require.NoError(t, pool.Link(ctx, testRepo))
	_, err = client.ReduplicateRepository(ctx, &gitalypb.ReduplicateRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	require.NoError(t, pool.Unlink(ctx, testRepo))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "cat-file", "-e", existingObjectID)
}
