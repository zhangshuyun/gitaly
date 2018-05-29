package repository

import (
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCalculateChecksum(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(path.Join(testRepoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags"} {
		require.NoError(t, os.MkdirAll(path.Join(testRepoPath, d), 0755))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-packed-refs", path.Join(testRepoPath, "packed-refs")).Run())

	request := &pb.CalculateChecksumRequest{Repository: testRepo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "7b5dbc8bbacb2bfd4584b5e26ed363e7a1cce041", response.Checksum)
}

func TestEmptyRepositoryCalculateChecksum(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	request := &pb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "0000000000000000000000000000000000000000", response.Checksum)
}

func TestBrokenRepositoryCalculateChecksum(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	repo, testRepoPath, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	// Force an empty HEAD file
	require.NoError(t, os.Truncate(path.Join(testRepoPath, "HEAD"), 0))

	request := &pb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	_, err := client.CalculateChecksum(testCtx, request)
	testhelper.RequireGrpcError(t, err, codes.DataLoss)
}

func TestFailedCalculateChecksum(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *pb.CalculateChecksumRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &pb.CalculateChecksumRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &pb.CalculateChecksumRequest{},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		testCtx, cancelCtx := testhelper.Context()
		defer cancelCtx()

		_, err := client.CalculateChecksum(testCtx, testCase.request)
		testhelper.RequireGrpcError(t, err, testCase.code)
	}
}
