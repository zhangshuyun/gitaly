package objectpool

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLink(t *testing.T) {
	cfg, repo, _, locator, client := setup(t, testserver.WithDisablePraefect())

	ctx, cancel := testhelper.Context()
	defer cancel()

	localRepo := localrepo.NewTestRepo(t, cfg, repo)

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist at start of test")
	require.NoError(t, pool.Create(ctx, repo), "create pool")

	// Mock object in the pool, which should be available to the pool members
	// after linking
	poolCommitID := gittest.WriteCommit(t, cfg, pool.FullPath(),
		gittest.WithBranch("pool-test-branch"))

	testCases := []struct {
		desc string
		req  *gitalypb.LinkRepositoryToObjectPoolRequest
		code codes.Code
	}{
		{
			desc: "Repository does not exist",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: nil,
				ObjectPool: pool.ToProto(),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Pool does not exist",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: repo,
				ObjectPool: nil,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Successful request",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: repo,
				ObjectPool: pool.ToProto(),
			},
			code: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.LinkRepositoryToObjectPool(ctx, tc.req)

			if tc.code != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.code)
				return
			}

			require.NoError(t, err, "error from LinkRepositoryToObjectPool")

			commit, err := localRepo.ReadCommit(ctx, git.Revision(poolCommitID))
			require.NoError(t, err)
			require.NotNil(t, commit)
			require.Equal(t, poolCommitID.String(), commit.Id)
		})
	}
}

func TestLinkIdempotent(t *testing.T) {
	cfg, repo, _, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()
	require.NoError(t, pool.Create(ctx, repo))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)
}

func TestLinkNoClobber(t *testing.T) {
	cfg, repo, repoPath, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.NoError(t, pool.Create(ctx, repo))

	alternatesFile := filepath.Join(repoPath, "objects/info/alternates")
	require.NoFileExists(t, alternatesFile)

	contentBefore := "mock/objects\n"
	require.NoError(t, ioutil.WriteFile(alternatesFile, []byte(contentBefore), 0644))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.Error(t, err)

	contentAfter := testhelper.MustReadFile(t, alternatesFile)
	require.Equal(t, contentBefore, string(contentAfter), "contents of existing alternates file should not have changed")
}

func TestLinkNoPool(t *testing.T) {
	cfg, repo, _, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	// intentionally do not call pool.Create
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	poolRepoPath, err := locator.GetRepoPath(pool)
	require.NoError(t, err)

	assert.True(t, storage.IsGitDirectory(poolRepoPath))
}

func TestUnlink(t *testing.T) {
	cfg, repo, _, locator, client := setup(t, testserver.WithDisablePraefect())

	ctx, cancel := testhelper.Context()
	defer cancel()

	deletedRepo, deletedRepoPath, removeDeletedRepo := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "todelete")
	defer removeDeletedRepo()

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	pool, err := objectpool.NewObjectPool(cfg, locator, gitCmdFactory, nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.NoError(t, pool.Create(ctx, repo), "create pool")
	require.NoError(t, pool.Link(ctx, repo))
	require.NoError(t, pool.Link(ctx, deletedRepo))

	removeDeletedRepo()
	require.NoFileExists(t, deletedRepoPath)

	pool2, err := objectpool.NewObjectPool(cfg, locator, gitCmdFactory, nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	require.NoError(t, pool2.Create(ctx, repo), "create pool 2")
	defer func() {
		require.NoError(t, pool2.Remove(ctx))
	}()

	require.False(t, gittest.RemoteExists(t, cfg, pool.FullPath(), repo.GlRepository), "sanity check: remote exists in pool")
	require.False(t, gittest.RemoteExists(t, cfg, pool.FullPath(), deletedRepo.GlRepository), "sanity check: remote exists in pool")

	testCases := []struct {
		desc string
		req  *gitalypb.UnlinkRepositoryFromObjectPoolRequest
		code codes.Code
	}{
		{
			desc: "Successful request",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: repo,
				ObjectPool: pool.ToProto(),
			},
			code: codes.OK,
		},
		{
			desc: "Not linked in the first place",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: repo,
				ObjectPool: pool2.ToProto(),
			},
			code: codes.OK,
		},
		{
			desc: "No Repository",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: nil,
				ObjectPool: pool.ToProto(),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "No ObjectPool",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: repo,
				ObjectPool: nil,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Repo not found",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: deletedRepo,
				ObjectPool: pool.ToProto(),
			},
			code: codes.OK,
		},
		{
			desc: "Pool not found",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  repo.GetStorageName(),
						RelativePath: gittest.NewObjectPoolName(t), // does not exist
					},
				},
			},
			code: codes.NotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.UnlinkRepositoryFromObjectPool(ctx, tc.req)

			if tc.code != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.code)
				return
			}

			require.NoError(t, err, "call UnlinkRepositoryFromObjectPool")

			remoteName := tc.req.Repository.GlRepository
			require.False(t, gittest.RemoteExists(t, cfg, pool.FullPath(), remoteName), "remote should no longer exist in pool")
		})
	}
}

func TestUnlinkIdempotent(t *testing.T) {
	cfg, repo, _, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), nil, repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()
	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))

	request := &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
		Repository: repo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.UnlinkRepositoryFromObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.UnlinkRepositoryFromObjectPool(ctx, request)
	require.NoError(t, err)
}
