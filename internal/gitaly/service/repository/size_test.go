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
	_, repo, _, client := setupRepositoryService(t)

	request := &gitalypb.RepositorySizeRequest{Repository: repo}

	ctx, cancel := testhelper.Context()
	defer cancel()
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

			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.RepositorySize(ctx, request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulGetObjectDirectorySizeRequest(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRepositoryService(t)
	repo.GitObjectDirectory = "objects/"

	ctx, cancel := testhelper.Context()
	defer cancel()

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

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("quarantined repo", func(t *testing.T) {
		repo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		quarantine, err := quarantine.New(ctx, repo, locator)
		require.NoError(t, err)

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: quarantine.QuarantinedRepo(),
		})
		require.NoError(t, err)
		require.NotNil(t, response)

		// Due to platform incompatibilities we can't assert the exact size of bytes: on
		// some, the directory entry is counted, on some it's not.
		require.Less(t, response.Size, int64(10))
	})

	t.Run("quarantined repo with different relative path", func(t *testing.T) {
		repo1, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		quarantine1, err := quarantine.New(ctx, repo1, locator)
		require.NoError(t, err)

		repo2, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		quarantine2, err := quarantine.New(ctx, repo2, locator)
		require.NoError(t, err)

		// We swap out the the object directories of both quarantines. So while both are
		// valid, we still expect that this RPC call fails because we detect that the
		// swapped-in quarantine directory does not belong to our repository.
		repo := proto.Clone(quarantine1.QuarantinedRepo()).(*gitalypb.Repository)
		repo.GitObjectDirectory = quarantine2.QuarantinedRepo().GetGitObjectDirectory()

		response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
			Repository: repo,
		})
		require.Error(t, err, "rpc error: code = InvalidArgument desc = GetObjectDirectoryPath: relative path escapes root directory")
		require.Nil(t, response)
	})
}
