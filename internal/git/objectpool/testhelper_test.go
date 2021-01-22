package objectpool

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	hooks.Override = "/"
	return m.Run()
}

func NewTestObjectPool(ctx context.Context, t *testing.T, storageName string) (*ObjectPool, func()) {
	pool, err := NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), storageName, testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	return pool, func() {
		require.NoError(t, pool.Remove(ctx))
	}
}
