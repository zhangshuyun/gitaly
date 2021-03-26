package remote

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

	request := &gitalypb.FindRemoteRootRefRequest{Repository: repo, Remote: "origin"}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.FindRemoteRootRef(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "master", response.Ref)
}

func TestFindRemoteRootRefFailedDueToValidation(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

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
			request: &gitalypb.FindRemoteRootRefRequest{Repository: repo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Remote is empty",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: repo, Remote: ""},
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
	_, repo, _, client := setupRemoteService(t)

	request := &gitalypb.FindRemoteRootRefRequest{Repository: repo, Remote: "invalid"}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	_, err := client.FindRemoteRootRef(testCtx, request)
	testhelper.RequireGrpcError(t, err, codes.Internal)
}
