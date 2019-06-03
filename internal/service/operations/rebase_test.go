package operations_test

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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
}

func TestFailedRebaseUserRebaseConfirmableRequestDueToInvalidHeader(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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

	server, serverSocketPath := runFullServer(t)
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

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for i, hookName := range operations.GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := operations.OverrideHooks(hookName, hookContent)
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

	server, serverSocketPath := runFullServer(t)
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
	require.Contains(t, firstResponse.GitError, "error: Failed to merge in the changes.")

	err = testhelper.ReceiveEOFWithTimeout(func() error {
		_, err = rebaseStream.Recv()
		return err
	})
	require.NoError(t, err, "consume EOF")

	newBranchSha := getBranchSha(t, testRepoPath, failedBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to GitError")
}

// DEPRECATED: https://gitlab.com/gitlab-org/gitaly/issues/1628
func TestSuccessfulUserRebaseRequest(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             rebaseUser,
		RebaseId:         "1",
		Branch:           []byte(branchName),
		BranchSha:        branchSha,
		RemoteRepository: testRepoCopy,
		RemoteBranch:     []byte("master"),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserRebase(ctx, request)
	require.NoError(t, err)

	newBranchSha := getBranchSha(t, testRepoPath, branchName)

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, response.RebaseSha)
}

// DEPRECATED: https://gitlab.com/gitlab-org/gitaly/issues/1628
func TestFailedUserRebaseRequestDueToPreReceiveError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	branchSha := getBranchSha(t, testRepoPath, branchName)

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             rebaseUser,
		Branch:           []byte(branchName),
		BranchSha:        branchSha,
		RemoteRepository: testRepoCopy,
		RemoteBranch:     []byte("master"),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1\n")
	for i, hookName := range operations.GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := operations.OverrideHooks(hookName, hookContent)
			require.NoError(t, err, "set up hooks override")
			defer remove()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			request.RebaseId = fmt.Sprintf("%d", i+1)
			response, err := client.UserRebase(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+rebaseUser.GlId)
		})
	}
}

// DEPRECATED: https://gitlab.com/gitlab-org/gitaly/issues/1628
func TestFailedUserRebaseRequestDueToGitError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	failedBranchName := "rebase-encoding-failure-trigger"
	branchSha := getBranchSha(t, testRepoPath, failedBranchName)

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             rebaseUser,
		RebaseId:         "1",
		Branch:           []byte(failedBranchName),
		BranchSha:        branchSha,
		RemoteRepository: testRepoCopy,
		RemoteBranch:     []byte("master"),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserRebase(ctx, request)
	require.NoError(t, err)
	require.Contains(t, response.GitError, "error: Failed to merge in the changes.")
}

// DEPRECATED: https://gitlab.com/gitlab-org/gitaly/issues/1628
func TestFailedUserRebaseRequestDueToValidations(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc    string
		request *gitalypb.UserRebaseRequest
		code    codes.Code
	}{
		{
			desc: "empty repository",
			request: &gitalypb.UserRebaseRequest{
				Repository:       nil,
				User:             rebaseUser,
				RebaseId:         "1",
				Branch:           []byte("some-branch"),
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty user",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             nil,
				RebaseId:         "1",
				Branch:           []byte("some-branch"),
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty rebase id",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             rebaseUser,
				RebaseId:         "",
				Branch:           []byte("some-branch"),
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             rebaseUser,
				RebaseId:         "1",
				Branch:           nil,
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch sha",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             rebaseUser,
				RebaseId:         "1",
				Branch:           []byte("some-branch"),
				BranchSha:        "",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty remote repository",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             rebaseUser,
				RebaseId:         "1",
				Branch:           []byte("some-branch"),
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: nil,
				RemoteBranch:     []byte("master"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty remote branch",
			request: &gitalypb.UserRebaseRequest{
				Repository:       testRepo,
				User:             rebaseUser,
				RebaseId:         "1",
				Branch:           []byte("some-branch"),
				BranchSha:        "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
				RemoteRepository: testRepoCopy,
				RemoteBranch:     nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			_, err := client.UserRebase(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
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

func buildHeaderRequest(repo *gitalypb.Repository, user *gitalypb.User, rebaseId string, branchName string, branchSha string, remoteRepo *gitalypb.Repository, remoteBranch string) *gitalypb.UserRebaseConfirmableRequest {
	return &gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			&gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       repo,
				User:             user,
				RebaseId:         rebaseId,
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
