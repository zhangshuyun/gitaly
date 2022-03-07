package objectpool

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestCreate(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, repo, _, _, client := setup(ctx, t)

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	poolReq := &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
	}

	_, err := client.CreateObjectPool(ctx, poolReq)
	require.NoError(t, err)

	pool = rewrittenObjectPool(ctx, t, cfg, pool)

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
	ctx := testhelper.Context(t)
	cfg, repo, _, _, client := setup(ctx, t, testserver.WithDisablePraefect())

	storageName := repo.GetStorageName()
	pool := initObjectPool(t, cfg, cfg.Storages[0])
	validPoolPath := pool.GetRelativePath()

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
			testhelper.RequireGrpcError(t, tc.error, err)
		})
	}
}

func TestDelete(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, repoProto, _, _, client := setup(ctx, t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repositoryClient := gitalypb.NewRepositoryServiceClient(extractConn(client))

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	_, err := client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repoProto,
	})
	require.NoError(t, err)

	validPoolPath := pool.GetRelativePath()

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
			testhelper.RequireGrpcError(t, tc.error, err)

			response, err := repositoryClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
				Repository: pool.ToProto().GetRepository(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.error != nil, response.GetExists())
		})
	}
}
