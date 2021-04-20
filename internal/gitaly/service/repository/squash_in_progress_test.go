package repository

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulIsSquashInProgressRequest(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "worktree", "add", "--detach", filepath.Join(repoPath, worktreePrefix, "squash-1"), "master")

	repoCopy, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanupFn()

	testCases := []struct {
		desc       string
		request    *gitalypb.IsSquashInProgressRequest
		inProgress bool
	}{
		{
			desc: "Squash in progress",
			request: &gitalypb.IsSquashInProgressRequest{
				Repository: repo,
				SquashId:   "1",
			},
			inProgress: true,
		},
		{
			desc: "no Squash in progress",
			request: &gitalypb.IsSquashInProgressRequest{
				Repository: repoCopy,
				SquashId:   "2",
			},
			inProgress: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.IsSquashInProgress(ctx, testCase.request)
			require.NoError(t, err)

			require.Equal(t, testCase.inProgress, response.InProgress)
		})
	}
}

func TestFailedIsSquashInProgressRequestDueToValidations(t *testing.T) {
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc    string
		request *gitalypb.IsSquashInProgressRequest
		code    codes.Code
	}{
		{
			desc:    "empty repository",
			request: &gitalypb.IsSquashInProgressRequest{SquashId: "1"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "empty Squash id",
			request: &gitalypb.IsSquashInProgressRequest{Repository: &gitalypb.Repository{}},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.IsSquashInProgress(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
