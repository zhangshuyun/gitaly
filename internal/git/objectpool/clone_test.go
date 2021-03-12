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

func setupObjectPool(t *testing.T) (*ObjectPool, *gitalypb.Repository) {
	t.Helper()

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	pool, err := NewObjectPool(cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg), repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Remove(context.TODO()); err != nil {
			panic(err)
		}
	})

	return pool, repo
}

func TestClone(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	require.NoError(t, pool.clone(ctx, testRepo))
	defer pool.Remove(ctx)

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	require.NoError(t, pool.clone(ctx, testRepo))
	defer pool.Remove(ctx)

	// re-clone on the directory
	require.Error(t, pool.clone(ctx, testRepo))
}
