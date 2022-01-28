package remote

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindRemoteRootRefSuccess(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRemoteService(t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	const (
		host   = "example.com"
		secret = "mysecret"
	)

	ctx := testhelper.Context(t)

	port, stopGitServer := gittest.HTTPServer(ctx, t, gitCmdFactory, repoPath, newGitRequestValidationMiddleware(host, secret))
	defer func() { require.NoError(t, stopGitServer()) }()

	originURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(repoPath))

	for _, tc := range []struct {
		desc    string
		request *gitalypb.FindRemoteRootRefRequest
	}{
		{
			desc: "with remote URL",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository:              repo,
				RemoteUrl:               originURL,
				HttpAuthorizationHeader: secret,
				HttpHost:                host,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.FindRemoteRootRef(ctx, tc.request)
			require.NoError(t, err)
			require.Equal(t, "master", response.Ref)
		})
	}
}

func TestFindRemoteRootRefWithUnbornRemoteHead(t *testing.T) {
	t.Parallel()
	cfg, remoteRepo, remoteRepoPath, client := setupRemoteService(t)

	// We're creating an empty repository. Empty repositories do have a HEAD set up, but they
	// point to an unborn branch because the default branch hasn't yet been created.
	_, clientRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "remote", "add", "foo", "file://"+clientRepoPath)
	ctx := testhelper.Context(t)

	response, err := client.FindRemoteRootRef(ctx, &gitalypb.FindRemoteRootRefRequest{
		Repository: remoteRepo,
		RemoteUrl:  "file://" + clientRepoPath,
	},
	)
	testhelper.RequireGrpcError(t, status.Error(codes.NotFound, "no remote HEAD found"), err)
	require.Nil(t, response)
}

func TestFindRemoteRootRefFailedDueToValidation(t *testing.T) {
	t.Parallel()

	// We're running tests with Praefect disabled given that we don't want to exercise
	// Praefect's validation, but Gitaly's.
	_, repo, _, client := setupRemoteService(t, testserver.WithDisablePraefect())

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc        string
		request     *gitalypb.FindRemoteRootRefRequest
		expectedErr error
	}{
		{
			desc: "Invalid repository",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: invalidRepo,
				RemoteUrl:  "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf("GetStorageByName: no such storage: \"fake\""),
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.FindRemoteRootRefRequest{
				RemoteUrl: "remote-url",
			},
			expectedErr: helper.ErrInvalidArgumentf("missing repository"),
		},
		{
			desc: "Remote URL is empty",
			request: &gitalypb.FindRemoteRootRefRequest{
				Repository: repo,
			},
			expectedErr: helper.ErrInvalidArgumentf("missing remote URL"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			_, err := client.FindRemoteRootRef(ctx, testCase.request)
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func TestFindRemoteRootRefFailedDueToInvalidRemote(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRemoteService(t)

	t.Run("invalid remote URL", func(t *testing.T) {
		fakeRepoDir := testhelper.TempDir(t)

		// We're using a nonexistent filepath remote URL so we avoid hitting the internet.
		request := &gitalypb.FindRemoteRootRefRequest{
			Repository: repo, RemoteUrl: "file://" + fakeRepoDir,
		}
		ctx := testhelper.Context(t)

		_, err := client.FindRemoteRootRef(ctx, request)
		testhelper.RequireGrpcCode(t, err, codes.Internal)
	})
}

func newGitRequestValidationMiddleware(host, secret string) func(http.ResponseWriter, *http.Request, http.Handler) {
	return func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		if r.Host != host {
			http.Error(w, "No Host", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != secret {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}
