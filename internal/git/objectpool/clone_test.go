package objectpool

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestClone(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, pool.clone(ctx, repo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, pool, repoProto := setupObjectPool(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	require.NoError(t, pool.clone(ctx, repo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	// re-clone on the directory
	require.Error(t, pool.clone(ctx, repo))
}
