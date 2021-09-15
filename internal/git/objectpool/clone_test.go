package objectpool

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func setupObjectPool(t *testing.T) (*ObjectPool, *gitalypb.Repository) {
	t.Helper()

	cfg, repo, _ := testcfg.BuildWithRepo(t)
	gitCommandFactory := git.NewExecCommandFactory(cfg)

	pool, err := NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		gitCommandFactory,
		catfile.NewCache(cfg),
		transaction.NewManager(cfg, backchannel.NewRegistry()),
		repo.GetStorageName(),
		gittest.NewObjectPoolName(t),
	)
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
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.DirExists(t, pool.FullPath())
	require.DirExists(t, filepath.Join(pool.FullPath(), "objects"))
}

func TestCloneExistingPool(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, testRepo := setupObjectPool(t)

	require.NoError(t, pool.clone(ctx, testRepo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	// re-clone on the directory
	require.Error(t, pool.clone(ctx, testRepo))
}
