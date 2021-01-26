package objectpool

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestClone(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	err = pool.clone(ctx, testRepo)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	err = pool.clone(ctx, testRepo)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	// Reclone on the directory
	err = pool.clone(ctx, testRepo)
	require.Error(t, err)
}
