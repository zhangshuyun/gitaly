package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func doesAlternatesExist(t *testing.T, repo repository.GitRepo) bool {
	altPath, err := git.AlternatesPath(repo)
	require.NoError(t, err)

	_, err = os.Stat(altPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		require.NoError(t, err)
	}
	return true
}

func TestDisconnectAlternates(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, _ := newRepositoryClient(t, serverSocketPath)

	pool, _ := objectpool.NewTestObjectPool(t)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	require.True(t, doesAlternatesExist(t, testRepo))

	_, err := client.DisconnectAlternates(ctx, &gitalypb.DisconnectAlternatesRequest{Repository: testRepo})
	require.NoError(t, err)

	assert.False(t, doesAlternatesExist(t, testRepo))

	_, err = client.DisconnectAlternates(ctx, &gitalypb.DisconnectAlternatesRequest{Repository: testRepo})
	require.NoError(t, err)

	assert.False(t, doesAlternatesExist(t, testRepo))
}
