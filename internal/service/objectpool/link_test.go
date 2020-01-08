package objectpool

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLink(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist at start of test")
	require.NoError(t, pool.Create(ctx, testRepo), "create pool")

	// Mock object in the pool, which should be available to the pool members
	// after linking
	poolCommitID := testhelper.CreateCommit(t, pool.FullPath(), "pool-test-branch", nil)

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
				Repository: testRepo,
				ObjectPool: nil,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Successful request",
			req: &gitalypb.LinkRepositoryToObjectPoolRequest{
				Repository: testRepo,
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

			commit, err := log.GetCommit(ctx, testRepo, poolCommitID)
			require.NoError(t, err)
			require.NotNil(t, commit)
			require.Equal(t, poolCommitID, commit.Id)
		})
	}
}

func TestLinkIdempotent(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)
	require.NoError(t, pool.Create(ctx, testRepo))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: testRepo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)
}

func TestLinkNoClobber(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))

	alternatesFile := filepath.Join(testRepoPath, "objects/info/alternates")
	testhelper.AssertPathNotExists(t, alternatesFile)

	contentBefore := "mock/objects\n"
	require.NoError(t, ioutil.WriteFile(alternatesFile, []byte(contentBefore), 0644))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: testRepo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.Error(t, err)

	contentAfter, err := ioutil.ReadFile(alternatesFile)
	require.NoError(t, err)

	require.Equal(t, contentBefore, string(contentAfter), "contents of existing alternates file should not have changed")
}

func TestLinkNoPool(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	// intentionally do not call pool.Create
	defer pool.Remove(ctx)

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: testRepo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	poolRepoPath, err := helper.GetRepoPath(pool)
	require.NoError(t, err)

	assert.True(t, helper.IsGitDirectory(poolRepoPath))
}

func TestUnlink(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	deletedRepo, deletedRepoPath, removeDeletedRepo := testhelper.NewTestRepo(t)
	defer removeDeletedRepo()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo), "create pool")
	require.NoError(t, pool.Link(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, deletedRepo))

	removeDeletedRepo()
	testhelper.AssertPathNotExists(t, deletedRepoPath)

	pool2, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	require.NoError(t, pool2.Create(ctx, testRepo), "create pool 2")
	defer pool2.Remove(ctx)

	require.False(t, testhelper.RemoteExists(t, pool.FullPath(), testRepo.GlRepository), "sanity check: remote exists in pool")
	require.False(t, testhelper.RemoteExists(t, pool.FullPath(), deletedRepo.GlRepository), "sanity check: remote exists in pool")

	testCases := []struct {
		desc string
		req  *gitalypb.UnlinkRepositoryFromObjectPoolRequest
		code codes.Code
	}{
		{
			desc: "Successful request",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: testRepo,
				ObjectPool: pool.ToProto(),
			},
			code: codes.OK,
		},
		{
			desc: "Not linked in the first place",
			req: &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
				Repository: testRepo,
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
				Repository: testRepo,
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
				Repository: testRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: &gitalypb.Repository{
						StorageName:  testRepo.GetStorageName(),
						RelativePath: "pool/does/not/exist",
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
			require.False(t, testhelper.RemoteExists(t, pool.FullPath(), remoteName), "remote should no longer exist in pool")
		})
	}
}

func TestUnlinkIdempotent(t *testing.T) {
	stop, serverSocketPath := runObjectPoolServer(t)
	defer stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)
	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	request := &gitalypb.UnlinkRepositoryFromObjectPoolRequest{
		Repository: testRepo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.UnlinkRepositoryFromObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.UnlinkRepositoryFromObjectPool(ctx, request)
	require.NoError(t, err)
}
