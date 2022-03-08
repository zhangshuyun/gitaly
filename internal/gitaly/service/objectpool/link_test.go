package objectpool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLink(t *testing.T) {
	testhelper.NewFeatureSets(featureflag.LinkRepositoryToObjectPoolNotFound).Run(t, testLink)
}

func testLink(t *testing.T, ctx context.Context) {
	cfg, repo, _, _, client := setup(ctx, t, testserver.WithDisablePraefect())

	localRepo := localrepo.NewTestRepo(t, cfg, repo)

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	require.NoError(t, pool.Remove(ctx), "make sure pool does not exist at start of test")
	require.NoError(t, pool.Create(ctx, localRepo), "create pool")

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
				testhelper.RequireGrpcCode(t, err, tc.code)
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
	testhelper.NewFeatureSets(featureflag.LinkRepositoryToObjectPoolNotFound).Run(t, testLinkIdempotent)
}

func testLinkIdempotent(t *testing.T, ctx context.Context) {
	cfg, repoProto, _, _, client := setup(ctx, t)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	_, err := client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repoProto,
	})
	require.NoError(t, err)

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repoProto,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	require.NoError(t, err)
}

func TestLinkNoClobber(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, _, client := setup(ctx, t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	require.NoError(t, pool.Create(ctx, repo))

	alternatesFile := filepath.Join(repoPath, "objects/info/alternates")
	require.NoFileExists(t, alternatesFile)

	contentBefore := "mock/objects\n"
	require.NoError(t, os.WriteFile(alternatesFile, []byte(contentBefore), 0o644))

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repoProto,
		ObjectPool: pool.ToProto(),
	}

	_, err := client.LinkRepositoryToObjectPool(ctx, request)
	require.Error(t, err)

	contentAfter := testhelper.MustReadFile(t, alternatesFile)
	require.Equal(t, contentBefore, string(contentAfter), "contents of existing alternates file should not have changed")
}

func TestLinkNoPool(t *testing.T) {
	testhelper.NewFeatureSets(featureflag.LinkRepositoryToObjectPoolNotFound).Run(t, testLinkNoPool)
}

func testLinkNoPool(t *testing.T, ctx context.Context) {
	cfg, repo, _, locator, client := setup(ctx, t)

	pool := initObjectPool(t, cfg, cfg.Storages[0])
	_, err := client.CreateObjectPool(ctx, &gitalypb.CreateObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
	})
	require.NoError(t, err)

	_, err = client.DeleteObjectPool(ctx, &gitalypb.DeleteObjectPoolRequest{
		ObjectPool: pool.ToProto(),
	})
	require.NoError(t, err)

	request := &gitalypb.LinkRepositoryToObjectPoolRequest{
		Repository: repo,
		ObjectPool: pool.ToProto(),
	}

	_, err = client.LinkRepositoryToObjectPool(ctx, request)
	if featureflag.LinkRepositoryToObjectPoolNotFound.IsEnabled(ctx) {
		testhelper.RequireGrpcCode(t, err, codes.NotFound)
		require.Error(t, err, "GetRepoPath: not a git repository:")
		return
	}

	require.NoError(t, err)

	pool = rewrittenObjectPool(ctx, t, cfg, pool)

	poolRepoPath, err := locator.GetRepoPath(pool)
	require.NoError(t, err)

	assert.True(t, storage.IsGitDirectory(poolRepoPath))
}
