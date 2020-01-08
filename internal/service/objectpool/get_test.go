package objectpool

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestGetObjectPoolSuccess(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	relativePoolPath := testhelper.NewTestObjectPoolName(t)

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), relativePoolPath)
	require.NoError(t, err)

	poolCtx, cancel := testhelper.Context()
	defer cancel()
	require.NoError(t, pool.Create(poolCtx, testRepo))
	require.NoError(t, pool.Link(poolCtx, testRepo))

	ctx, cancel := testhelper.Context()
	defer pool.Remove(ctx)
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: testRepo,
	})

	require.NoError(t, err)
	require.Equal(t, relativePoolPath, resp.GetObjectPool().GetRepository().GetRelativePath())
}

func TestGetObjectPoolNoFile(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: testRepo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}

func TestGetObjectPoolBadFile(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	alternatesFilePath := filepath.Join(testRepoPath, "objects", "info", "alternates")
	require.NoError(t, os.MkdirAll(filepath.Dir(alternatesFilePath), 0755))
	require.NoError(t, ioutil.WriteFile(alternatesFilePath, []byte("not-a-directory"), 0644))

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: testRepo,
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetObjectPool())
}
