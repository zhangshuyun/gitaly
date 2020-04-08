package operations_test

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var (
	rebaseUser = &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	branchName = "many_files"
)

func TestSuccessfulUserRebaseConfirmableRequest(t *testing.T) {
	pushOptions := []string{"ci.skip", "test=value"}

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := operations.SetupAndStartGitlabServer(t, rebaseUser.GlId, testRepo.GlRepository, pushOptions...)
	defer cleanupSrv()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	preReceiveHookOutputPath, removePreReceive := testhelper.WriteEnvToCustomHook(t, testRepoPath, "pre-receive")
	postReceiveHookOutputPath, removePostReceive := testhelper.WriteEnvToCustomHook(t, testRepoPath, "post-receive")
	defer removePreReceive()
	defer removePostReceive()

	headerRequest := buildHeaderRequest(testRepo, rebaseUser, "1", branchName, branchSha, testRepoCopy, "master")
	headerRequest.GetHeader().GitPushOptions = pushOptions
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = gitlog.GetCommit(ctx, testRepo, firstResponse.GetRebaseSha())
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	err = testhelper.ReceiveEOFWithTimeout(func() error {
		_, err = rebaseStream.Recv()
		return err
	})
	require.NoError(t, err, "consume EOF")

	newBranchSha := getBranchSha(t, testRepoPath, branchName)

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")

	for _, outputPath := range []string{preReceiveHookOutputPath, postReceiveHookOutputPath} {
		output := string(testhelper.MustReadFile(t, outputPath))
		require.Contains(t, output, "GIT_PUSH_OPTION_COUNT=2")
		require.Contains(t, output, "GIT_PUSH_OPTION_0=ci.skip")
		require.Contains(t, output, "GIT_PUSH_OPTION_1=test=value")
	}
}

func TestFailedRebaseUserRebaseConfirmableRequestDueToInvalidHeader(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := operations.SetupAndStartGitlabServer(t, rebaseUser.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testCases := []struct {
		desc string
		req  *gitalypb.UserRebaseConfirmableRequest
	}{
		{
			desc: "empty Repository",
			req:  buildHeaderRequest(nil, rebaseUser, "1", branchName, branchSha, testRepoCopy, "master"),
		},
		{
			desc: "empty User",
			req:  buildHeaderRequest(testRepo, nil, "1", branchName, branchSha, testRepoCopy, "master"),
		},
		{
			desc: "empty Branch",
			req:  buildHeaderRequest(testRepo, rebaseUser, "1", "", branchSha, testRepoCopy, "master"),
		},
		{
			desc: "empty BranchSha",
			req:  buildHeaderRequest(testRepo, rebaseUser, "1", branchName, "", testRepoCopy, "master"),
		},
		{
			desc: "empty RemoteRepository",
			req:  buildHeaderRequest(testRepo, rebaseUser, "1", branchName, branchSha, nil, "master"),
		},
		{
			desc: "empty RemoteBranch",
			req:  buildHeaderRequest(testRepo, rebaseUser, "1", branchName, branchSha, testRepoCopy, ""),
		},
		{
			desc: "invalid branch name",
			req:  buildHeaderRequest(testRepo, rebaseUser, "1", branchName, branchSha, testRepoCopy, "+dev:master"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(tc.req), "send request header")

			firstResponse, err := rebaseStream.Recv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
			require.Empty(t, firstResponse.GetRebaseSha(), "rebase sha on first response")
		})
	}
}

func TestAbortedUserRebaseConfirmable(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testCases := []struct {
		req       *gitalypb.UserRebaseConfirmableRequest
		closeSend bool
		desc      string
		code      codes.Code
	}{
		{req: &gitalypb.UserRebaseConfirmableRequest{}, desc: "empty request, don't close", code: codes.FailedPrecondition},
		{req: &gitalypb.UserRebaseConfirmableRequest{}, closeSend: true, desc: "empty request and close", code: codes.FailedPrecondition},
		{closeSend: true, desc: "no request just close", code: codes.Internal},
	}

	for i, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			cleanupSrv := operations.SetupAndStartGitlabServer(t, rebaseUser.GlId, testRepo.GlRepository)
			defer cleanupSrv()

			testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			branchSha := getBranchSha(t, testRepoPath, branchName)

			headerRequest := buildHeaderRequest(testRepo, rebaseUser, fmt.Sprintf("%v", i), branchName, branchSha, testRepoCopy, "master")

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(headerRequest), "send first request")

			firstResponse, err := rebaseStream.Recv()
			require.NoError(t, err, "receive first response")
			require.NotEmpty(t, firstResponse.GetRebaseSha(), "rebase sha on first response")

			if tc.req != nil {
				require.NoError(t, rebaseStream.Send(tc.req), "send second request")
			}

			if tc.closeSend {
				require.NoError(t, rebaseStream.CloseSend(), "close request stream from client")
			}

			secondResponse, err := recvTimeout(rebaseStream, 1*time.Second)
			if err == errRecvTimeout {
				t.Fatal(err)
			}

			require.False(t, secondResponse.GetRebaseApplied(), "rebase should not have been applied")
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.code)

			newBranchSha := getBranchSha(t, testRepoPath, branchName)
			require.Equal(t, newBranchSha, branchSha, "branch should not change when the rebase is aborted")
		})
	}
}

func TestFailedUserRebaseConfirmableDueToApplyBeingFalse(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := operations.SetupAndStartGitlabServer(t, rebaseUser.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(testRepo, rebaseUser, "1", branchName, branchSha, testRepoCopy, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = gitlog.GetCommit(ctx, testRepo, firstResponse.GetRebaseSha())
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(false)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.Error(t, err, "second response should have error")
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
	require.False(t, secondResponse.GetRebaseApplied(), "the second rebase is not applied")

	newBranchSha := getBranchSha(t, testRepoPath, branchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase is not applied")
	require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase is not applied")
}

func TestFailedUserRebaseConfirmableRequestDueToPreReceiveError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := operations.SetupAndStartGitlabServer(t, rebaseUser.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for i, hookName := range operations.GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err, "set up hooks override")
			defer remove()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildHeaderRequest(testRepo, rebaseUser, fmt.Sprintf("%v", i), branchName, branchSha, testRepoCopy, "master")
			require.NoError(t, rebaseStream.Send(headerRequest), "send header")

			firstResponse, err := rebaseStream.Recv()
			require.NoError(t, err, "receive first response")

			_, err = gitlog.GetCommit(ctx, testRepo, firstResponse.GetRebaseSha())
			require.NoError(t, err, "look up git commit before rebase is applied")

			applyRequest := buildApplyRequest(true)
			require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

			secondResponse, err := rebaseStream.Recv()

			require.NoError(t, err, "receive second response")
			require.Contains(t, secondResponse.PreReceiveError, "failure")

			err = testhelper.ReceiveEOFWithTimeout(func() error {
				_, err = rebaseStream.Recv()
				return err
			})
			require.NoError(t, err, "consume EOF")

			newBranchSha := getBranchSha(t, testRepoPath, branchName)
			require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to PreReceiveError")
			require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase fails due to PreReceiveError")
		})
	}
}

func TestFailedUserRebaseConfirmableDueToGitError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServerWithHooks(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	failedBranchName := "rebase-encoding-failure-trigger"
	branchSha := getBranchSha(t, testRepoPath, failedBranchName)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(testRepo, rebaseUser, "1", failedBranchName, branchSha, testRepoCopy, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")
	require.Contains(t, firstResponse.GitError, "CONFLICT (content): Merge conflict in README.md")

	err = testhelper.ReceiveEOFWithTimeout(func() error {
		_, err = rebaseStream.Recv()
		return err
	})
	require.NoError(t, err, "consume EOF")

	newBranchSha := getBranchSha(t, testRepoPath, failedBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to GitError")
}

func getBranchSha(t *testing.T, repoPath string, branchName string) string {
	branchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", branchName))
	return strings.TrimSpace(branchSha)
}

// This error is used as a sentinel value
var errRecvTimeout = errors.New("timeout waiting for response")

func recvTimeout(bidi gitalypb.OperationService_UserRebaseConfirmableClient, timeout time.Duration) (*gitalypb.UserRebaseConfirmableResponse, error) {
	type responseError struct {
		response *gitalypb.UserRebaseConfirmableResponse
		err      error
	}
	responseCh := make(chan responseError, 1)

	go func() {
		resp, err := bidi.Recv()
		responseCh <- responseError{resp, err}
	}()

	select {
	case respErr := <-responseCh:
		return respErr.response, respErr.err
	case <-time.After(timeout):
		return nil, errRecvTimeout
	}
}

func buildHeaderRequest(repo *gitalypb.Repository, user *gitalypb.User, rebaseID string, branchName string, branchSha string, remoteRepo *gitalypb.Repository, remoteBranch string) *gitalypb.UserRebaseConfirmableRequest {
	return &gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       repo,
				User:             user,
				RebaseId:         rebaseID,
				Branch:           []byte(branchName),
				BranchSha:        branchSha,
				RemoteRepository: remoteRepo,
				RemoteBranch:     []byte(remoteBranch),
			},
		},
	}
}

func buildApplyRequest(apply bool) *gitalypb.UserRebaseConfirmableRequest {
	return &gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Apply{
			Apply: apply,
		},
	}
}
