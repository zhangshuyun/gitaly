package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

// We assume that the combined size of the Git objects in the test
// repository, even in optimally packed state, is greater than this.
const testRepoMinSizeKB = 10000

func TestSuccessfulRepositorySizeRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	repo := testhelper.TestRepository()

	request := &gitalypb.RepositorySizeRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response, err := client.RepositorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)
}

func TestFailedRepositorySizeRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description string
		repo        *gitalypb.Repository
	}{
		{repo: invalidRepo, description: "Invalid repo"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.RepositorySizeRequest{
				Repository: testCase.repo,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepositorySize(ctx, request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulGetObjectDirectorySizeRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo := testhelper.TestRepository()
	testRepo.GitObjectDirectory = "objects/"

	request := &gitalypb.GetObjectDirectorySizeRequest{
		Repository: testRepo,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	response, err := client.GetObjectDirectorySize(ctx, request)
	require.NoError(t, err)

	require.True(t,
		response.Size > testRepoMinSizeKB,
		"repository size %d should be at least %d", response.Size, testRepoMinSizeKB,
	)
}
