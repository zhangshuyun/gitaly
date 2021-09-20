package objectpool

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestGetObjectPoolSuccess(t *testing.T) {
	cfg, repo, _, _, client := setup(t)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	relativePoolPath := pool.GetRelativePath()

	poolCtx, cancel := testhelper.Context()
	defer cancel()
	require.NoError(t, pool.Create(poolCtx, repo))
	require.NoError(t, pool.Link(poolCtx, repo))

	ctx, cancel := testhelper.Context()
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Equal(t, relativePoolPath, resp.GetObjectPool().GetRepository().GetRelativePath())
}

func TestGetObjectPoolNoFile(t *testing.T) {
	_, repoo, _, _, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repoo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}

func TestGetObjectPoolBadFile(t *testing.T) {
	_, repo, repoPath, _, client := setup(t)

	alternatesFilePath := filepath.Join(repoPath, "objects", "info", "alternates")
	require.NoError(t, os.MkdirAll(filepath.Dir(alternatesFilePath), 0o755))
	require.NoError(t, ioutil.WriteFile(alternatesFilePath, []byte("not-a-directory"), 0o644))

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}
