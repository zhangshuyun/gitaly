package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// We assume that the combined size of the Git objects in the test
// repository, even in optimally packed state, is greater than this.
const testRepoMinSizeKB = 10000

func TestSuccessfulRepositorySizeRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	request := &gitalypb.RepositorySizeRequest{Repository: repo}
	response, err := client.RepositorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)
}

func TestFailedRepositorySizeRequest(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		description string
		repo        *gitalypb.Repository
	}{
		{
			description: "Invalid repo",
			repo:        &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.RepositorySizeRequest{
				Repository: testCase.repo,
			}
			ctx := testhelper.Context(t)
			_, err := client.RepositorySize(ctx, request)
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulGetObjectDirectorySizeRequest(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)
	repo.GitObjectDirectory = "objects/"

	request := &gitalypb.GetObjectDirectorySizeRequest{Repository: repo}
	response, err := client.GetObjectDirectorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)
}

func TestGetObjectDirectorySize_quarantine(t *testing.T) {
	t.Parallel()

	cfg, client := setupRepositoryServiceWithoutRepo(t)
	locator := config.NewLocator(cfg)
	ctx := testhelper.Context(t)

	t.Run("quarantined repo", func(t *testing.T) {
		repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})

		quarantine, err := quarantine.New(ctx, gittest.RewrittenRepository(ctx, t, cfg, repo), locator)
		require.NoError(t, err)

		// quarantine.New in Gitaly would receive an already rewritten repository. Gitaly would then calculate
		// the quarantine directories based on the rewritten relative path. That quarantine would then be looped
		// through Rails, which would then send a request with the quarantine object directories set based on the
		// rewritten relative path but with the original relative path of the repository. Since we're using the production
		// helpers here, we need to manually substitute the rewritten relative path with the original one when sending
		// it back through the API.
		quarantinedRepo := quarantine.QuarantinedRepo()
		quarantinedRepo.RelativePath = repo.RelativePath

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: quarantinedRepo,
		})
		require.NoError(t, err)
		require.NotNil(t, response)

		// Due to platform incompatibilities we can't assert the exact size of bytes: on
		// some, the directory entry is counted, on some it's not.
		require.Less(t, response.Size, int64(10))
	})

	t.Run("quarantined repo with different relative path", func(t *testing.T) {
		repo1, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})
		quarantine1, err := quarantine.New(ctx, gittest.RewrittenRepository(ctx, t, cfg, repo1), locator)
		require.NoError(t, err)

		repo2, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})
		quarantine2, err := quarantine.New(ctx, gittest.RewrittenRepository(ctx, t, cfg, repo2), locator)
		require.NoError(t, err)

		// We swap out the the object directories of both quarantines. So while both are
		// valid, we still expect that this RPC call fails because we detect that the
		// swapped-in quarantine directory does not belong to our repository.
		repo := proto.Clone(quarantine1.QuarantinedRepo()).(*gitalypb.Repository)
		repo.GitObjectDirectory = quarantine2.QuarantinedRepo().GetGitObjectDirectory()
		// quarantine.New in Gitaly would receive an already rewritten repository. Gitaly would then calculate
		// the quarantine directories based on the rewritten relative path. That quarantine would then be looped
		// through Rails, which would then send a request with the quarantine object directories set based on the
		// rewritten relative path but with the original relative path of the repository. Since we're using the production
		// helpers here, we need to manually substitute the rewritten relative path with the original one when sending
		// it back through the API.
		repo.RelativePath = repo1.RelativePath

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: repo,
		})
		require.Error(t, err, "rpc error: code = InvalidArgument desc = GetObjectDirectoryPath: relative path escapes root directory")
		require.Nil(t, response)
	})
}
