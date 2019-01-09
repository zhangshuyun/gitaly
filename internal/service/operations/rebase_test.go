package operations_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

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

	branchName := "many_files"
	branchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName))
	branchSha = strings.TrimSpace(branchSha)

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             user,
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

	newBranchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName))
	newBranchSha = strings.TrimSpace(newBranchSha)

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, response.RebaseSha)
}

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

	branchName := "many_files"
	branchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName))
	branchSha = strings.TrimSpace(branchSha)

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             user,
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
			require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)
		})
	}
}

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

	branchName := "rebase-encoding-failure-trigger"
	branchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName))
	branchSha = strings.TrimSpace(branchSha)

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	request := &gitalypb.UserRebaseRequest{
		Repository:       testRepo,
		User:             user,
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
	require.Contains(t, response.GitError, "error: Failed to merge in the changes.")
}

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

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	testCases := []struct {
		desc    string
		request *gitalypb.UserRebaseRequest
		code    codes.Code
	}{
		{
			desc: "empty repository",
			request: &gitalypb.UserRebaseRequest{
				Repository:       nil,
				User:             user,
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
				User:             user,
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
				User:             user,
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
				User:             user,
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
				User:             user,
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
				User:             user,
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
