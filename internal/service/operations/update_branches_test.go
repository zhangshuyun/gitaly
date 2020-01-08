package operations

import (
	"context"
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	updateBranchName = "feature"
	newrev           = []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f")
	oldrev           = []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9")
)

func TestSuccessfulUserUpdateBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserUpdateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(updateBranchName),
		Newrev:     newrev,
		Oldrev:     oldrev,
		User:       user,
	}

	response, err := client.UserUpdateBranch(ctx, request)

	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)

	branchCommit, err := log.GetCommit(ctx, testRepo, updateBranchName)

	require.NoError(t, err)
	require.Equal(t, string(newrev), branchCommit.Id)
}

func TestSuccessfulGitHooksForUserUpdateBranchRequest(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
			defer cleanupSrv()

			hookOutputTempPath, cleanup := WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(updateBranchName),
				Newrev:     newrev,
				Oldrev:     oldrev,
				User:       user,
			}

			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserUpdateBranchDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserUpdateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(updateBranchName),
		Newrev:     newrev,
		Oldrev:     oldrev,
		User:       user,
	}
	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove, err := WriteCustomHook(testRepoPath, hookName, hookContent)
		require.NoError(t, err)
		defer remove()

		ctx, cancel := testhelper.Context()
		defer cancel()

		response, err := client.UserUpdateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+user.GlUsername)
		require.Contains(t, response.PreReceiveError, "PWD="+testRepoPath)
	}
}

func TestFailedUserUpdateBranchRequest(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	revDoesntExist := fmt.Sprintf("%x", sha1.Sum([]byte("we need a non existent sha")))

	testCases := []struct {
		desc       string
		branchName string
		newrev     []byte
		oldrev     []byte
		user       *gitalypb.User
		code       codes.Code
	}{
		{
			desc:       "empty branch name",
			branchName: "",
			newrev:     newrev,
			oldrev:     oldrev,
			user:       user,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "empty newrev",
			branchName: updateBranchName,
			newrev:     nil,
			oldrev:     oldrev,
			user:       user,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "empty oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     nil,
			user:       user,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "empty user",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     oldrev,
			user:       nil,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "non-existing branch",
			branchName: "i-dont-exist",
			newrev:     newrev,
			oldrev:     oldrev,
			user:       user,
			code:       codes.FailedPrecondition,
		},
		{
			desc:       "non-existing newrev",
			branchName: updateBranchName,
			newrev:     []byte(revDoesntExist),
			oldrev:     oldrev,
			user:       user,
			code:       codes.FailedPrecondition,
		},
		{
			desc:       "non-existing oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     []byte(revDoesntExist),
			user:       user,
			code:       codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchName),
				Newrev:     testCase.newrev,
				Oldrev:     testCase.oldrev,
				User:       testCase.user,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.UserUpdateBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
