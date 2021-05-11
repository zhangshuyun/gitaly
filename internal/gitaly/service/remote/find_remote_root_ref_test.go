package remote

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	cfg, repo, repoPath, client := setupRemoteService(t)

	originURL := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "remote", "get-url", "origin"))

	for _, tc := range []struct {
		desc    string
		request *gitalypb.FindRemoteRootRefRequest
	}{
		{
			desc:    "with remote name",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: repo, Remote: "origin"},
		},
		{
			desc:    "with remote URL",
			request: &gitalypb.FindRemoteRootRefRequest{Repository: repo, RemoteUrl: originURL},
		},
		{
			// Unfortunately, we do not really have a nice way to verify we actually got
			// the auth header. So this test case only really verifies that it doesn't
			// break the world to set up one.
			desc: "with remote URL and auth header",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository:              repo,
				RemoteUrl:               originURL,
				HttpAuthorizationHeader: "mysecret",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.FindRemoteRootRef(ctx, tc.request)
			require.NoError(t, err)
			require.Equal(t, "master", response.Ref)
		})
	}
}

func TestFindRemoteRootRefWithUnbornRemoteHead(t *testing.T) {
	cfg, remoteRepo, remoteRepoPath, client := setupRemoteService(t)

	// We're creating an empty repository. Empty repositories do have a HEAD set up, but they
	// point to an unborn branch because the default branch hasn't yet been created.
	_, clientRepoPath, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer cleanup()
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "remote", "add",
		"foo", "file://"+clientRepoPath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, request := range []*gitalypb.FindRemoteRootRefRequest{
		&gitalypb.FindRemoteRootRefRequest{Repository: remoteRepo, Remote: "foo"},
		&gitalypb.FindRemoteRootRefRequest{Repository: remoteRepo, RemoteUrl: "file://" + clientRepoPath},
	} {
		response, err := client.FindRemoteRootRef(ctx, request)
		require.Equal(t, status.Error(codes.NotFound, "no remote HEAD found"), err)
		require.Nil(t, response)
	}
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
			desc: "Remote name and URL is empty",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: repo,
			},
			expectedErr: []error{
				status.Error(codes.InvalidArgument, "got neither remote name nor URL"),
			},
		},
		{
			desc: "Remote name and URL is set",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: repo,
				Remote:     "remote-name",
				RemoteUrl:  "remote-url",
			},
			expectedErr: []error{
				status.Error(codes.InvalidArgument, "got remote name and URL"),
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

	t.Run("invalid remote name", func(t *testing.T) {
		request := &gitalypb.FindRemoteRootRefRequest{Repository: repo, Remote: "invalid"}
		ctx, cancel := testhelper.Context()
		defer cancel()

		_, err := client.FindRemoteRootRef(ctx, request)
		testhelper.RequireGrpcError(t, err, codes.Internal)
	})

	t.Run("invalid remote URL", func(t *testing.T) {
		fakeRepoDir := testhelper.TempDir(t)

		// We're using a nonexistent filepath remote URL so we avoid hitting the internet.
		request := &gitalypb.FindRemoteRootRefRequest{
			Repository: repo, RemoteUrl: "file://" + fakeRepoDir,
		}

		ctx, cancel := testhelper.Context()
		defer cancel()

		_, err := client.FindRemoteRootRef(ctx, request)
		testhelper.RequireGrpcError(t, err, codes.Internal)
	})
}
