package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestGetObjectPoolSuccess(t *testing.T) {
	cfg, repoProto, _, _, client := setup(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	relativePoolPath := pool.GetRelativePath()

	poolCtx := testhelper.Context(t)
	require.NoError(t, pool.Create(poolCtx, repo))
	require.NoError(t, pool.Link(poolCtx, repo))

	ctx := testhelper.Context(t)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repoProto,
	})

	require.NoError(t, err)
	require.Equal(t, relativePoolPath, resp.GetObjectPool().GetRepository().GetRelativePath())
}

func TestGetObjectPoolNoFile(t *testing.T) {
	_, repoo, _, _, client := setup(t)
	ctx := testhelper.Context(t)

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
	require.NoError(t, os.WriteFile(alternatesFilePath, []byte("not-a-directory"), 0o644))
	ctx := testhelper.Context(t)

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}
