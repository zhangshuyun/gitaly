package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulIsRebaseInProgressRequest(t *testing.T) {
	t.Parallel()
	cfg, repo1, repoPath1, client := setupRepositoryService(t)

	gittest.Exec(t, cfg, "-C", repoPath1, "worktree", "add", "--detach", filepath.Join(repoPath1, worktreePrefix, fmt.Sprintf("%s-1", rebaseWorktreePrefix)), "master")

	brokenPath := filepath.Join(repoPath1, worktreePrefix, fmt.Sprintf("%s-2", rebaseWorktreePrefix))
	gittest.Exec(t, cfg, "-C", repoPath1, "worktree", "add", "--detach", brokenPath, "master")
	require.NoError(t, os.Chmod(brokenPath, 0))
	require.NoError(t, os.Chtimes(brokenPath, time.Now(), time.Now().Add(-16*time.Minute)))

	oldPath := filepath.Join(repoPath1, worktreePrefix, fmt.Sprintf("%s-3", rebaseWorktreePrefix))
	gittest.Exec(t, cfg, "-C", repoPath1, "worktree", "add", "--detach", oldPath, "master")
	require.NoError(t, os.Chtimes(oldPath, time.Now(), time.Now().Add(-16*time.Minute)))

	repo2, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "second")
	t.Cleanup(cleanupFn)

	testCases := []struct {
		desc       string
		request    *gitalypb.IsRebaseInProgressRequest
		inProgress bool
	}{
		{
			desc: "rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: repo1,
				RebaseId:   "1",
			},
			inProgress: true,
		},
		{
			desc: "broken rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: repo1,
				RebaseId:   "2",
			},
			inProgress: false,
		},
		{
			desc: "expired rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: repo1,
				RebaseId:   "3",
			},
			inProgress: false,
		},
		{
			desc: "no rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: repo2,
				RebaseId:   "2",
			},
			inProgress: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.IsRebaseInProgress(ctx, testCase.request)
			require.NoError(t, err)

			require.Equal(t, testCase.inProgress, response.InProgress)
		})
	}
}

func TestFailedIsRebaseInProgressRequestDueToValidations(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc    string
		request *gitalypb.IsRebaseInProgressRequest
		code    codes.Code
	}{
		{
			desc:    "empty repository",
			request: &gitalypb.IsRebaseInProgressRequest{RebaseId: "1"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "empty rebase id",
			request: &gitalypb.IsRebaseInProgressRequest{Repository: &gitalypb.Repository{}},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.IsRebaseInProgress(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
