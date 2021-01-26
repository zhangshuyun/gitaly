package objectpool

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/status"
)

func TestCreate(t *testing.T) {
	locator := config.NewLocator(config.Config)
	server, serverSocketPath := runObjectPoolServer(t, config.Config, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(config.Config, locator, git.NewExecCommandFactory(config.Config), "default", testhelper.NewTestObjectPoolName(t))
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
	_, err = os.Stat(filepath.Join(pool.FullPath(), "hooks"))
	assert.True(t, os.IsNotExist(err))

	// No problems
	out := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "cat-file", "-s", "55bc176024cfa3baaceb71db584c7e5df900ea65")
	assert.Equal(t, "282\n", string(out))

	// Making the same request twice, should result in an error
	_, err = client.CreateObjectPool(ctx, poolReq)
	require.Error(t, err)
	require.True(t, pool.IsValid())
}

func TestUnsuccessfulCreate(t *testing.T) {
	locator := config.NewLocator(config.Config)
	server, serverSocketPath := runObjectPoolServer(t, config.Config, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	validPoolPath := testhelper.NewTestObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(config.Config, locator, git.NewExecCommandFactory(config.Config), "default", validPoolPath)
	require.NoError(t, err)
	defer pool.Remove(ctx)

	testCases := []struct {
		desc    string
		request *gitalypb.CreateObjectPoolRequest
		error   error
	}{
		{
			desc: "no origin repository",
			request: &gitalypb.CreateObjectPoolRequest{
				ObjectPool: pool.ToProto(),
			},
			error: errMissingOriginRepository,
		},
		{
			desc: "no object pool",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
			},
			error: errMissingPool,
		},
		{
			desc: "outside pools directory",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  "default",
						RelativePath: "outside-pools",
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "path must be lowercase",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  "default",
						RelativePath: strings.ToUpper(validPoolPath),
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "subdirectories must match first four pool digits",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  "default",
						RelativePath: "@pools/aa/bb/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.git",
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "pool path traversal fails",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: testRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  "default",
						RelativePath: validPoolPath + "/..",
					},
				},
			},
			error: errInvalidPoolDir,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CreateObjectPool(ctx, tc.request)
			require.Equal(t, status.Convert(tc.error).Err(), err)
		})
	}
}

func TestDelete(t *testing.T) {
	locator := config.NewLocator(config.Config)
	server, serverSocketPath := runObjectPoolServer(t, config.Config, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	validPoolPath := testhelper.NewTestObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(config.Config, locator, git.NewExecCommandFactory(config.Config), "default", validPoolPath)
	require.NoError(t, err)
	require.NoError(t, pool.Create(ctx, testRepo))

	for _, tc := range []struct {
		desc         string
		relativePath string
		error        error
	}{
		{
			desc:         "deleting outside pools directory fails",
			relativePath: ".",
			error:        errInvalidPoolDir,
		},
		{
			desc:         "deleting pools directory fails",
			relativePath: "@pools",
			error:        errInvalidPoolDir,
		},
		{
			desc:         "deleting first level subdirectory fails",
			relativePath: "@pools/ab",
			error:        errInvalidPoolDir,
		},
		{
			desc:         "deleting second level subdirectory fails",
			relativePath: "@pools/ab/cd",
			error:        errInvalidPoolDir,
		},
		{
			desc:         "deleting pool subdirectory fails",
			relativePath: filepath.Join(validPoolPath, "objects"),
			error:        errInvalidPoolDir,
		},
		{
			desc:         "path traversing fails",
			relativePath: validPoolPath + "/../../../../..",
			error:        errInvalidPoolDir,
		},
		{
			desc:         "deleting pool succeeds",
			relativePath: validPoolPath,
		},
		{
			desc:         "deleting non-existent pool succeeds",
			relativePath: validPoolPath,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.DeleteObjectPool(ctx, &gitalypb.DeleteObjectPoolRequest{ObjectPool: &gitalypb.ObjectPool{
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: tc.relativePath,
				},
			}})
			require.Equal(t, status.Convert(tc.error).Err(), err)
		})
	}
}
