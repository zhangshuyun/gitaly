package remote

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestFindRemoteRootRefWithUnbornRemoteHead(t *testing.T) {
	cfg, remoteRepo, remoteRepoPath, client := setupRemoteService(t)

	// We're creating an empty repository. Empty repositories do have a HEAD set up, but they
	// point to an unborn branch because the default branch hasn't yet been created.
	_, clientRepoPath, cleanup := gittest.InitBareRepoAt(t, cfg.Storages[0])
	defer cleanup()
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "remote", "add",
		"foo", "file://"+clientRepoPath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := client.FindRemoteRootRef(ctx, &gitalypb.FindRemoteRootRefRequest{
		Repository: remoteRepo,
		Remote:     "foo",
	})
	require.Equal(t, status.Error(codes.NotFound, "no remote HEAD found"), err)
	require.Nil(t, response)
}

func TestFindRemoteRootRefFailedDueToValidation(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc        string
		request     *gitalypb.FindRemoteRootRefRequest
		expectedErr []error
	}{
		{
			desc: "Invalid repository",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: invalidRepo,
				Remote:     "remote-name",
			},
			expectedErr: []error{
				status.Error(codes.InvalidArgument, "GetStorageByName: no such storage: \"fake\""),
				status.Error(codes.InvalidArgument, "repo scoped: invalid Repository"),
			},
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.FindRemoteRootRefRequest{
				Remote: "remote-name",
			},
			expectedErr: []error{
				status.Error(codes.InvalidArgument, "missing repository"),
				status.Error(codes.InvalidArgument, "repo scoped: empty Repository"),
			},
		},
		{
			desc: "Remote is empty",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: repo,
			},
			expectedErr: []error{
				status.Error(codes.InvalidArgument, "empty remote can't be queried"),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.FindRemoteRootRef(ctx, testCase.request)
			// We cannot test for equality given that some errors depend on whether we
			// proxy via Praefect or not. We thus simply assert that the actual error is
			// one of the possible errors, which is the same as equality for all the
			// other tests.
			require.Contains(t, testCase.expectedErr, err)
		})
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
