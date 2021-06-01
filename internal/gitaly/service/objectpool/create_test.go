package objectpool

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/status"
)

func TestCreate(t *testing.T) {
	cfg, repo, _, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)

	poolReq := &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
	}

	_, err = client.CreateObjectPool(ctx, poolReq)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	// Checks if the underlying repository is valid
	require.True(t, pool.IsValid())

	// No hooks
	assert.NoDirExists(t, filepath.Join(pool.FullPath(), "hooks"))

	// No problems
	out := gittest.Exec(t, cfg, "-C", pool.FullPath(), "cat-file", "-s", "55bc176024cfa3baaceb71db584c7e5df900ea65")
	assert.Equal(t, "282\n", string(out))

	// Making the same request twice, should result in an error
	_, err = client.CreateObjectPool(ctx, poolReq)
	require.Error(t, err)
	require.True(t, pool.IsValid())
}

func TestUnsuccessfulCreate(t *testing.T) {
	cfg, repo, _, locator, client := setup(t, testserver.WithDisablePraefect())

	ctx, cancel := testhelper.Context()
	defer cancel()

	validPoolPath := gittest.NewObjectPoolName(t)
	storageName := repo.GetStorageName()
	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, storageName, validPoolPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

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
				Origin: repo,
			},
			error: errMissingPool,
		},
		{
			desc: "outside pools directory",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  storageName,
						RelativePath: "outside-pools",
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "path must be lowercase",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  storageName,
						RelativePath: strings.ToUpper(validPoolPath),
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "subdirectories must match first four pool digits",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  storageName,
						RelativePath: "@pools/aa/bb/ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.git",
					},
				},
			},
			error: errInvalidPoolDir,
		},
		{
			desc: "pool path traversal fails",
			request: &gitalypb.CreateObjectPoolRequest{
				Origin: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  storageName,
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
	cfg, repo, _, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	validPoolPath := gittest.NewObjectPoolName(t)
	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), validPoolPath)
	require.NoError(t, err)
	require.NoError(t, pool.Create(ctx, repo))

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
					StorageName:  repo.GetStorageName(),
					RelativePath: tc.relativePath,
				},
			}})
			require.Equal(t, status.Convert(tc.error).Err(), err)
		})
	}
}
