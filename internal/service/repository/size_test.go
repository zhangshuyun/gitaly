package repository

import (
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulRepositorySizeRequest(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t)
	defer conn.Close()

	storageName := "default"
	storagePath, found := config.StoragePath(storageName)
	if !found {
		t.Fatalf("No %q storage was found", storageName)
	}

	repoCopyPath := path.Join(storagePath, "fixed-size-repo.git")
	testhelper.MustRunCommand(t, nil, "cp", "-R", "testdata/fixed-size-repo.git", repoCopyPath)
	defer os.RemoveAll(repoCopyPath)

	request := &pb.RepositorySizeRequest{
		Repository: &pb.Repository{
			StorageName:  storageName,
			RelativePath: "fixed-size-repo.git",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response, err := client.RepositorySize(ctx, request)
	require.NoError(t, err)
	// We can't test for an exact size because it will be different for systems with different sector sizes,
	// so we settle for anything greater than zero.
	require.True(t, response.Size > 0, "size must be greater than zero")
}

func TestFailedRepositorySizeRequest(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t)
	defer conn.Close()

	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description string
		repo        *pb.Repository
	}{
		{repo: invalidRepo, description: "Invalid repo"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			request := &pb.RepositorySizeRequest{
				Repository: testCase.repo,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepositorySize(ctx, request)
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
		})
	}
}
