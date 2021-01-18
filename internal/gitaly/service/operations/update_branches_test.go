package operations

import (
	"context"
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	updateBranchName = "feature"
	newrev           = []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f")
	oldrev           = []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9")
)

func TestSuccessfulUserUpdateBranchRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
	}).Run(t, testSuccessfulUserUpdateBranchRequest)
}

func testSuccessfulUserUpdateBranchRequest(t *testing.T, ctx context.Context) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc             string
		updateBranchName string
		oldRev           []byte
		newRev           []byte
	}{
		{
			desc:             "short name fast-forward update",
			updateBranchName: updateBranchName,
			oldRev:           []byte("0b4bc9a49b562e85de7cc9e834518ea6828729b9"),
			newRev:           []byte("1a35b5a77cf6af7edf6703f88e82f6aff613666f"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			responseOk := &gitalypb.UserUpdateBranchResponse{}
			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.updateBranchName),
				Newrev:     testCase.newRev,
				Oldrev:     testCase.oldRev,
				User:       testhelper.TestUser,
			}
			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			require.Equal(t, responseOk, response)

			branchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(testCase.updateBranchName))

			require.NoError(t, err)
			require.Equal(t, string(testCase.newRev), branchCommit.Id)
		})
	}
}

func TestSuccessfulGitHooksForUserUpdateBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testSuccessfulGitHooksForUserUpdateBranchRequest(t, ctx)
}

func testSuccessfulGitHooksForUserUpdateBranchRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			request := &gitalypb.UserUpdateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(updateBranchName),
				Newrev:     newrev,
				Oldrev:     oldrev,
				User:       testhelper.TestUser,
			}

			responseOk := &gitalypb.UserUpdateBranchResponse{}
			response, err := client.UserUpdateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			require.Equal(t, responseOk, response)
			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserUpdateBranchDueToHooks(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserUpdateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(updateBranchName),
		Newrev:     newrev,
		Oldrev:     oldrev,
		User:       testhelper.TestUser,
	}
	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' - >&2\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
		defer remove()

		response, err := client.UserUpdateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		require.Contains(t, response.PreReceiveError, "PWD="+testRepoPath)

		responseOk := &gitalypb.UserUpdateBranchResponse{
			PreReceiveError: response.PreReceiveError,
		}
		require.Equal(t, responseOk, response)
	}
}

func TestFailedUserUpdateBranchRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	revDoesntExist := fmt.Sprintf("%x", sha1.Sum([]byte("we need a non existent sha")))

	testCases := []struct {
		desc       string
		branchName string
		newrev     []byte
		oldrev     []byte
		user       *gitalypb.User
		err        error
	}{
		{
			desc:       "empty branch name",
			branchName: "",
			newrev:     newrev,
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty branch name"),
		},
		{
			desc:       "empty newrev",
			branchName: updateBranchName,
			newrev:     nil,
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty newrev"),
		},
		{
			desc:       "empty oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     nil,
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty oldrev"),
		},
		{
			desc:       "empty user",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     oldrev,
			user:       nil,
			err:        status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc:       "non-existing branch",
			branchName: "i-dont-exist",
			newrev:     newrev,
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", "i-dont-exist"),
		},
		{
			desc:       "non-existing newrev",
			branchName: updateBranchName,
			newrev:     []byte(revDoesntExist),
			oldrev:     oldrev,
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", updateBranchName),
		},
		{
			desc:       "non-existing oldrev",
			branchName: updateBranchName,
			newrev:     newrev,
			oldrev:     []byte(revDoesntExist),
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %v. Please refresh and try again.", updateBranchName),
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

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserUpdateBranch(ctx, request)
			require.Nil(t, response)
			require.Equal(t, testCase.err, err)
		})
	}
}
