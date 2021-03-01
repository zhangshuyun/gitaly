package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulIsRebaseInProgressRequest(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo1, testRepo1Path, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepo1Path, "worktree", "add", "--detach", filepath.Join(testRepo1Path, worktreePrefix, fmt.Sprintf("%s-1", rebaseWorktreePrefix)), "master")

	brokenPath := filepath.Join(testRepo1Path, worktreePrefix, fmt.Sprintf("%s-2", rebaseWorktreePrefix))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepo1Path, "worktree", "add", "--detach", brokenPath, "master")
	os.Chmod(brokenPath, 0)
	os.Chtimes(brokenPath, time.Now(), time.Now().Add(-16*time.Minute))
	defer func() {
		os.Chmod(brokenPath, 0755)
		os.RemoveAll(brokenPath)
	}()

	oldPath := filepath.Join(testRepo1Path, worktreePrefix, fmt.Sprintf("%s-3", rebaseWorktreePrefix))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepo1Path, "worktree", "add", "--detach", oldPath, "master")
	os.Chtimes(oldPath, time.Now(), time.Now().Add(-16*time.Minute))

	testRepo2, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		request    *gitalypb.IsRebaseInProgressRequest
		inProgress bool
	}{
		{
			desc: "rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: testRepo1,
				RebaseId:   "1",
			},
			inProgress: true,
		},
		{
			desc: "broken rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: testRepo1,
				RebaseId:   "2",
			},
			inProgress: false,
		},
		{
			desc: "expired rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: testRepo1,
				RebaseId:   "3",
			},
			inProgress: false,
		},
		{
			desc: "no rebase in progress",
			request: &gitalypb.IsRebaseInProgressRequest{
				Repository: testRepo2,
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
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

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
