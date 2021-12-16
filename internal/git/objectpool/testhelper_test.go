package objectpool

import (
	"context"
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

func TestMain(m *testing.M) {
	testhelper.Run(m, testhelper.WithSetup(func() error {
		config.OverrideHooksPath = "/"
		return nil
	}))
}

func setupObjectPool(t *testing.T, ctx context.Context) (*ObjectPool, *gitalypb.Repository) {
	t.Helper()

	cfg, repo, _ := testcfg.BuildWithRepo(t)
	gitCommandFactory := git.NewExecCommandFactory(cfg)

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	pool, err := NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		gitCommandFactory,
		catfileCache,
		transaction.NewManager(cfg, backchannel.NewRegistry()),
		repo.GetStorageName(),
		gittest.NewObjectPoolName(t),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := pool.Remove(ctx); err != nil {
			panic(err)
		}
	})

	return pool, repo
}
