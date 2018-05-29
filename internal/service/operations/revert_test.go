package operations_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulUserRevertRequest(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	destinationBranch := "revert-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", destinationBranch, "master")

	masterHeadCommit, err := log.GetCommit(ctxOuter, testRepo, "master", "")
	require.NoError(t, err)

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, "d59c60028b053793cecfb4022de34602e1a9218e", "")
	require.NoError(t, err)

	testRepoCopy, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc         string
		request      *pb.UserRevertRequest
		branchUpdate *pb.OperationBranchUpdate
	}{
		{
			desc: "branch exists",
			request: &pb.UserRevertRequest{
				Repository: testRepo,
				User:       user,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			branchUpdate: &pb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository",
			request: &pb.UserRevertRequest{
				Repository:      testRepo,
				User:            user,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-1"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &pb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository",
			request: &pb.UserRevertRequest{
				Repository:      testRepo,
				User:            user,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-2"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartRepository: testRepoCopy,
				StartBranchName: []byte("master"),
			},
			branchUpdate: &pb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository",
			request: &pb.UserRevertRequest{
				Repository:      testRepo,
				User:            user,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-3"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &pb.OperationBranchUpdate{BranchCreated: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			response, err := client.UserRevert(ctx, testCase.request)
			require.NoError(t, err)

			headCommit, err := log.GetCommit(ctx, testRepo, string(testCase.request.BranchName), "")
			require.NoError(t, err)

			expectedBranchUpdate := testCase.branchUpdate
			expectedBranchUpdate.CommitId = headCommit.Id

			require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
			require.Equal(t, testCase.request.Message, headCommit.Subject)
			require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
		})
	}
}

func TestSuccessfulGitHooksForUserRevertRequest(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	destinationBranch := "revert-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", destinationBranch, "master")

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, "d59c60028b053793cecfb4022de34602e1a9218e", "")
	require.NoError(t, err)

	request := &pb.UserRevertRequest{
		Repository: testRepo,
		User:       user,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	for _, hookName := range operations.GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			hookPath, hookOutputTempPath := operations.WriteEnvToHook(t, testRepoPath, hookName)
			defer os.Remove(hookPath)
			defer os.Remove(hookOutputTempPath)

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			response, err := client.UserRevert(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_ID="+user.GlId)
			require.Contains(t, output, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserRevertRequestDueToValidations(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, "d59c60028b053793cecfb4022de34602e1a9218e", "")
	require.NoError(t, err)

	destinationBranch := "revert-dst"

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	testCases := []struct {
		desc    string
		request *pb.UserRevertRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &pb.UserRevertRequest{
				Repository: testRepo,
				User:       nil,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit",
			request: &pb.UserRevertRequest{
				Repository: testRepo,
				User:       user,
				Commit:     nil,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch name",
			request: &pb.UserRevertRequest{
				Repository: testRepo,
				User:       user,
				Commit:     revertedCommit,
				BranchName: nil,
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty message",
			request: &pb.UserRevertRequest{
				Repository: testRepo,
				User:       user,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			_, err := client.UserRevert(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFailedUserRevertRequestDueToPreReceiveError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	destinationBranch := "revert-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", destinationBranch, "master")

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, "d59c60028b053793cecfb4022de34602e1a9218e", "")
	require.NoError(t, err)

	request := &pb.UserRevertRequest{
		Repository: testRepo,
		User:       user,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range operations.GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			hookPath := path.Join(testRepoPath, "hooks", hookName)
			require.NoError(t, ioutil.WriteFile(hookPath, hookContent, 0755))
			defer os.Remove(hookPath)

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			response, err := client.UserRevert(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)
		})
	}
}

func TestFailedUserRevertRequestDueToCreateTreeError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	destinationBranch := "revert-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", destinationBranch, "master")

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	// This revert patch of the following commit cannot be applied to the destinationBranch above
	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, "372ab6950519549b14d220271ee2322caa44d4eb", "")
	require.NoError(t, err)

	request := &pb.UserRevertRequest{
		Repository: testRepo,
		User:       user,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Gitlab::Git::Repository::CreateTreeError", response.CreateTreeError)
}

func TestFailedUserRevertRequestDueToCommitError(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	sourceBranch := "revert-src"
	destinationBranch := "revert-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", destinationBranch, "master")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", sourceBranch, "a5391128b0ef5d21df5dd23d98557f4ef12fae20")

	user := &pb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	revertedCommit, err := log.GetCommit(ctxOuter, testRepo, sourceBranch, "")
	require.NoError(t, err)

	request := &pb.UserRevertRequest{
		Repository:      testRepo,
		User:            user,
		Commit:          revertedCommit,
		BranchName:      []byte(destinationBranch),
		Message:         []byte("Reverting " + revertedCommit.Id),
		StartBranchName: []byte(sourceBranch),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Branch diverged", response.CommitError)
}
