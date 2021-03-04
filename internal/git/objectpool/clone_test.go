package objectpool

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func setupObjectPool(t *testing.T) (*ObjectPool, *gitalypb.Repository, func()) {
	t.Helper()

	var deferrer testhelper.Deferrer
	defer deferrer.Call()

	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	deferrer.Add(cleanup)

	pool, err := NewObjectPool(cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg), repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	deferrer.Add(func() {
		if err := pool.Remove(context.TODO()); err != nil {
			panic(err)
		}
	})

	cleaner := deferrer.Relocate()
	return pool, repo, cleaner.Call
}

func TestClone(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo, cleanup := setupObjectPool(t)
	defer cleanup()

	require.NoError(t, pool.clone(ctx, testRepo))
	defer pool.Remove(ctx)

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo, cleanup := setupObjectPool(t)
	defer cleanup()

	require.NoError(t, pool.clone(ctx, testRepo))
	defer pool.Remove(ctx)

	// re-clone on the directory
	require.Error(t, pool.clone(ctx, testRepo))
}
