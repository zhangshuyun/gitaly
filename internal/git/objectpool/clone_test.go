package objectpool

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestClone(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, pool, testRepo := setupObjectPool(t, ctx)

	require.NoError(t, pool.clone(ctx, testRepo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, pool, testRepo := setupObjectPool(t, ctx)

	require.NoError(t, pool.clone(ctx, testRepo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	// re-clone on the directory
	require.Error(t, pool.clone(ctx, testRepo))
}
