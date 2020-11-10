package objectpool

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}

func NewTestObjectPool(ctx context.Context, t *testing.T, storageName string) (*ObjectPool, func()) {
	pool, err := NewObjectPool(config.Config, storageName, testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	return pool, func() {
		require.NoError(t, pool.Remove(ctx))
	}
}
