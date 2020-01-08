package operations

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserCreateBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	startPoint := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	startPointCommit, err := log.GetCommit(ctx, testRepo, startPoint)
	require.NoError(t, err)

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	testCases := []struct {
		desc           string
		branchName     string
		startPoint     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:       "valid branch",
			branchName: "new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("new-branch"),
				TargetCommit: startPointCommit,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			branchName := testCase.branchName
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       user,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateBranch(ctx, request)
			if testCase.expectedBranch != nil {
				defer exec.Command("git", "-C", testRepoPath, "branch", "-D", branchName).Run()
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch)
			require.Empty(t, response.PreReceiveError)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.Contains(t, string(branches), branchName)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateBranchRequest(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchName := "new-branch"
	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchName),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       user,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer exec.Command("git", "-C", testRepoPath, "branch", "-D", branchName).Run()

			hookOutputTempPath, cleanup := WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserCreateBranchDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte("new-branch"),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       user,
	}
	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove, err := WriteCustomHook(testRepoPath, hookName, hookContent)
		require.NoError(t, err)
		defer remove()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		response, err := client.UserCreateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+user.GlUsername)
	}
}

func TestFailedUserCreateBranchRequest(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	testCases := []struct {
		desc       string
		branchName string
		startPoint string
		user       *gitalypb.User
		code       codes.Code
	}{
		{
			desc:       "empty start_point",
			branchName: "shiny-new-branch",
			startPoint: "",
			user:       user,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "empty user",
			branchName: "shiny-new-branch",
			startPoint: "master",
			user:       nil,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "non-existing starting point",
			branchName: "new-branch",
			startPoint: "i-dont-exist",
			user:       user,
			code:       codes.FailedPrecondition,
		},

		{
			desc:       "branch exists",
			branchName: "master",
			startPoint: "master",
			user:       user,
			code:       codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testCase.user,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.UserCreateBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestSuccessfulUserDeleteBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"

	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	user := &gitalypb.User{
		Name:  []byte("Alejandro Rodríguez"),
		Email: []byte("alejandro@gitlab.com"),
		GlId:  "user-123",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       user,
	}

	_, err := client.UserDeleteBranch(ctx, request)
	require.NoError(t, err)

	branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
	require.NotContains(t, string(branches), branchNameInput, "branch name still exists in branches list")
}

func TestSuccessfulGitHooksForUserDeleteBranchRequest(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	user := &gitalypb.User{
		Name:       []byte("Alejandro Rodríguez"),
		Email:      []byte("alejandro@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "johndoe",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       user,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)

			hookOutputTempPath, cleanup := WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteBranch(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_USERNAME=johndoe")
		})
	}
}

func TestFailedUserDeleteBranchDueToValidation(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:  []byte("Alejandro Rodríguez"),
		Email: []byte("alejandro@gitlab.com"),
		GlId:  "user-123",
	}

	testCases := []struct {
		desc    string
		request *gitalypb.UserDeleteBranchRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte("does-matter-the-name-if-user-is-empty"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       user,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "non-existent branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       user,
				BranchName: []byte("i-do-not-exist"),
			},
			code: codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteBranch(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFailedUserDeleteBranchDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	user := &gitalypb.User{
		Name:  []byte("Alejandro Rodríguez"),
		Email: []byte("alejandro@gitlab.com"),
		GlId:  "user-123",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       user,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserDeleteBranch(ctx, request)
			require.Nil(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.Contains(t, string(branches), branchNameInput, "branch name does not exist in branches list")
		})
	}
}
