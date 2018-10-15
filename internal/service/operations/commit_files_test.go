package operations_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var (
	user = &gitalypb.User{
		Name:  []byte("John Doe"),
		Email: []byte("johndoe@gitlab.com"),
		GlId:  "user-1",
	}
	commitFilesMessage = []byte("Change files")
)

func TestSuccessfulUserCommitFilesRequest(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	newRepo, newRepoPath, newRepoCleanupFn := testhelper.InitBareRepo(t)
	defer newRepoCleanupFn()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	filePath := "héllo/wörld"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	testCases := []struct {
		desc            string
		repo            *gitalypb.Repository
		repoPath        string
		branchName      string
		repoCreated     bool
		branchCreated   bool
		executeFilemode bool
	}{
		{
			desc:          "existing repo and branch",
			repo:          testRepo,
			repoPath:      testRepoPath,
			branchName:    "feature",
			repoCreated:   false,
			branchCreated: false,
		},
		{
			desc:          "existing repo, new branch",
			repo:          testRepo,
			repoPath:      testRepoPath,
			branchName:    "new-branch",
			repoCreated:   false,
			branchCreated: true,
		},
		{
			desc:          "new repo",
			repo:          newRepo,
			repoPath:      newRepoPath,
			branchName:    "feature",
			repoCreated:   true,
			branchCreated: true,
		},
		{
			desc:            "create executable file",
			repo:            testRepo,
			repoPath:        testRepoPath,
			branchName:      "feature-executable",
			repoCreated:     false,
			branchCreated:   true,
			executeFilemode: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := metadata.NewOutgoingContext(ctxOuter, md)
			headerRequest := headerRequest(tc.repo, user, tc.branchName, commitFilesMessage, authorName, authorEmail)
			actionsRequest1 := createFileHeaderRequest(filePath)
			actionsRequest2 := actionContentRequest("My")
			actionsRequest3 := actionContentRequest(" content")
			actionsRequest4 := chmodFileHeaderRequest(filePath, tc.executeFilemode)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))
			require.NoError(t, stream.Send(actionsRequest3))
			require.NoError(t, stream.Send(actionsRequest4))

			r, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.repoCreated, r.GetBranchUpdate().GetRepoCreated())
			require.Equal(t, tc.branchCreated, r.GetBranchUpdate().GetBranchCreated())

			headCommit, err := log.GetCommit(ctxOuter, tc.repo, tc.branchName)
			require.NoError(t, err)
			require.Equal(t, authorName, headCommit.Author.Name)
			require.Equal(t, user.Name, headCommit.Committer.Name)
			require.Equal(t, authorEmail, headCommit.Author.Email)
			require.Equal(t, user.Email, headCommit.Committer.Email)
			require.Equal(t, commitFilesMessage, headCommit.Subject)

			fileContent := testhelper.MustRunCommand(t, nil, "git", "-C", tc.repoPath, "show", headCommit.GetId()+":"+filePath)
			require.Equal(t, "My content", string(fileContent))

			commitInfo := testhelper.MustRunCommand(t, nil, "git", "-C", tc.repoPath, "show", headCommit.GetId())
			expectedFilemode := "100644"
			if tc.executeFilemode {
				expectedFilemode = "100755"
			}
			require.Contains(t, string(commitInfo), fmt.Sprint("new file mode ", expectedFilemode))
		})
	}
}

func TestFailedUserCommitFilesRequestDueToHooks(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	branchName := "feature"
	filePath := "my/file.txt"
	headerRequest := headerRequest(testRepo, user, branchName, commitFilesMessage, nil, nil)
	actionsRequest1 := createFileHeaderRequest(filePath)
	actionsRequest2 := actionContentRequest("My content")
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range operations.GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			hookPath := path.Join(testRepoPath, "hooks", hookName)
			ioutil.WriteFile(hookPath, hookContent, 0755)
			defer os.Remove(hookPath)

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))

			r, err := stream.CloseAndRecv()
			require.NoError(t, err)

			require.Contains(t, r.PreReceiveError, "GL_ID="+user.GlId)
			require.Contains(t, r.PreReceiveError, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserCommitFilesRequestDueToIndexError(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)
	testCases := []struct {
		desc       string
		requests   []*gitalypb.UserCommitFilesRequest
		indexError string
	}{
		{
			desc: "file already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, user, "feature", commitFilesMessage, nil, nil),
				createFileHeaderRequest("README.md"),
				actionContentRequest("This file already exists"),
			},
			indexError: "A file with this name already exists",
		},
		{
			desc: "file doesn't exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, user, "feature", commitFilesMessage, nil, nil),
				chmodFileHeaderRequest("documents/story.txt", true),
			},
			indexError: "A file with this name doesn't exist",
		},
		{
			desc: "dir already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, user, "utf-dir", commitFilesMessage, nil, nil),
				actionRequest(&gitalypb.UserCommitFilesAction{
					UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
						Header: &gitalypb.UserCommitFilesActionHeader{
							Action:        gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
							Base64Content: false,
							FilePath:      []byte("héllo"),
						},
					},
				}),
				actionContentRequest("This file already exists, as a directory"),
			},
			indexError: "A directory with this name already exists",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			for _, req := range tc.requests {
				require.NoError(t, stream.Send(req))
			}

			r, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.indexError, r.GetIndexError())
		})
	}
}

func TestFailedUserCommitFilesRequest(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := operations.NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)
	branchName := "feature"
	testCases := []struct {
		desc string
		req  *gitalypb.UserCommitFilesRequest
	}{
		{
			desc: "empty Repository",
			req:  headerRequest(nil, user, branchName, commitFilesMessage, nil, nil),
		},
		{
			desc: "empty User",
			req:  headerRequest(testRepo, nil, branchName, commitFilesMessage, nil, nil),
		},
		{
			desc: "empty BranchName",
			req:  headerRequest(testRepo, user, "", commitFilesMessage, nil, nil),
		},
		{
			desc: "empty CommitMessage",
			req:  headerRequest(testRepo, user, branchName, nil, nil, nil),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(tc.req))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage, authorName, authorEmail []byte) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Header{
			Header: &gitalypb.UserCommitFilesRequestHeader{
				Repository:        repo,
				User:              user,
				BranchName:        []byte(branchName),
				CommitMessage:     commitMessage,
				CommitAuthorName:  authorName,
				CommitAuthorEmail: authorEmail,
				StartBranchName:   nil,
				StartRepository:   nil,
			},
		},
	}
}

func createFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: false,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func chmodFileHeaderRequest(filePath string, executeFilemode bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:          gitalypb.UserCommitFilesActionHeader_CHMOD,
				FilePath:        []byte(filePath),
				ExecuteFilemode: executeFilemode,
			},
		},
	})
}

func actionContentRequest(content string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Content{
			Content: []byte(content),
		},
	})
}

func actionRequest(action *gitalypb.UserCommitFilesAction) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Action{
			Action: action,
		},
	}
}
