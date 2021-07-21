package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
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

	cfg, repo, _, client := setupRepositoryService(t)
	locator := config.NewLocator(cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	quarantine, err := quarantine.New(ctx, repo, locator)
	require.NoError(t, err)

	response, err := client.GetObjectDirectorySize(ctx, &gitalypb.GetObjectDirectorySizeRequest{
		Repository: quarantine.QuarantinedRepo(),
	})
	require.EqualError(t, err, "rpc error: code = InvalidArgument desc = GetObjectDirectoryPath: relative path escapes root directory")
	require.Nil(t, response)
}
