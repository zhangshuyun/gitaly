package remote

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.FindRemoteRootRefRequest{Repository: testRepo, Remote: "origin"}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.FindRemoteRootRef(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "master", response.Ref)
}

func TestFindRemoteRootRefFailedDueToValidation(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *gitalypb.FindRemoteRootRefRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.FindRemoteRootRefRequest{},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Remote is nil",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: testRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Remote is empty",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: testRepo, Remote: ""},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		testCtx, cancelCtx := testhelper.Context()
		defer cancelCtx()

		_, err := client.FindRemoteRootRef(testCtx, testCase.request)
		testhelper.RequireGrpcError(t, err, testCase.code)
	}
}

func TestFindRemoteRootRefFailedDueToInvalidRemote(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.FindRemoteRootRefRequest{Repository: testRepo, Remote: "invalid"}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	_, err := client.FindRemoteRootRef(testCtx, request)
	testhelper.RequireGrpcError(t, err, codes.Internal)
}
