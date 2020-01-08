package objectpool

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestCreate(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool("default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	poolReq := &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     testRepo,
	}

	_, err = client.CreateObjectPool(ctx, poolReq)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	// Checks if the underlying repository is valid
	require.True(t, pool.IsValid())

	// No hooks
	_, err = os.Stat(path.Join(pool.FullPath(), "hooks"))
	assert.True(t, os.IsNotExist(err))

	// No problems
	out := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "cat-file", "-s", "55bc176024cfa3baaceb71db584c7e5df900ea65")
	assert.Equal(t, "282\n", string(out))

	// No automatic GC
	gc := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "config", "--get", "gc.auto")
	assert.Equal(t, "0\n", string(gc))

	// Making the same request twice, should result in an error
	_, err = client.CreateObjectPool(ctx, poolReq)
	require.Error(t, err)
	require.True(t, pool.IsValid())
}

func TestUnsuccessfulCreate(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool("default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	testCases := []struct {
		desc    string
		request *gitalypb.CreateObjectPoolRequest
		code    codes.Code
	}{
		{
			desc: "no origin repository",
			request: &gitalypb.CreateObjectPoolRequest{
				ObjectPool: pool.ToProto(),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "no object pool",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CreateObjectPool(ctx, tc.request)
			require.Equal(t, tc.code, helper.GrpcCode(err))
		})
	}
}

func TestRemove(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool("default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	require.NoError(t, pool.Create(ctx, testRepo))

	req := &gitalypb.DeleteObjectPoolRequest{
		ObjectPool: pool.ToProto(),
	}

	_, err = client.DeleteObjectPool(ctx, req)
	require.NoError(t, err)

	// Removing again should be possible
	_, err = client.DeleteObjectPool(ctx, req)
	require.NoError(t, err)
}
