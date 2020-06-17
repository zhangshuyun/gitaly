package operations

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

var (
	commitFilesMessage = []byte("Change files")
)

func testSuccessfulUserCommitFilesRequest(t *testing.T, ctxWithFeatureFlags context.Context) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

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
			ctx := metadata.NewOutgoingContext(ctxWithFeatureFlags, md)
			headerRequest := headerRequest(tc.repo, testhelper.TestUser, tc.branchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, authorName, authorEmail)

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

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.repoCreated, resp.GetBranchUpdate().GetRepoCreated())
			require.Equal(t, tc.branchCreated, resp.GetBranchUpdate().GetBranchCreated())

			headCommit, err := log.GetCommit(ctxWithFeatureFlags, tc.repo, tc.branchName)
			require.NoError(t, err)
			require.Equal(t, authorName, headCommit.Author.Name)
			require.Equal(t, testhelper.TestUser.Name, headCommit.Committer.Name)
			require.Equal(t, authorEmail, headCommit.Author.Email)
			require.Equal(t, testhelper.TestUser.Email, headCommit.Committer.Email)
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

func TestSuccessfulUserCommitFilesRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testSuccessfulUserCommitFilesRequest(t, ctx)
}

func TestSuccessfulUserCommitFilesRequestMove(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	branchName := "master"
	previousFilePath := "README"
	filePath := "NEWREADME"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")

	for i, tc := range []struct {
		content string
		infer   bool
	}{
		{content: "", infer: false},
		{content: "foo", infer: false},
		{content: "", infer: true},
		{content: "foo", infer: true},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			origFileContent := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show", branchName+":"+previousFilePath)
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)
			headerRequest := headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, authorName, authorEmail)
			actionsRequest1 := moveFileHeaderRequest(previousFilePath, filePath, tc.infer)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))

			if len(tc.content) > 0 {
				actionsRequest2 := actionContentRequest(tc.content)
				require.NoError(t, stream.Send(actionsRequest2))
			}

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			update := resp.GetBranchUpdate()
			require.NotNil(t, update)

			fileContent := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show", update.CommitId+":"+filePath)

			if tc.infer {
				require.Equal(t, string(origFileContent), string(fileContent))
			} else {
				require.Equal(t, tc.content, string(fileContent))
			}
		})
	}
}

func TestSuccessfulUserCommitFilesRequestForceCommit(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	targetBranchName := "feature"
	startBranchName := []byte("master")

	startBranchCommit, err := log.GetCommit(ctxOuter, testRepo, string(startBranchName))
	require.NoError(t, err)

	targetBranchCommit, err := log.GetCommit(ctxOuter, testRepo, targetBranchName)
	require.NoError(t, err)

	mergeBaseOut := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "merge-base", targetBranchCommit.Id, startBranchCommit.Id)
	mergeBaseID := text.ChompBytes(mergeBaseOut)
	require.NotEqual(t, mergeBaseID, targetBranchCommit.Id, "expected %s not to be an ancestor of %s", targetBranchCommit.Id, startBranchCommit.Id)

	headerRequest := headerRequest(testRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
	setAuthorAndEmail(headerRequest, authorName, authorEmail)
	setStartBranchName(headerRequest, startBranchName)
	setForce(headerRequest, true)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := log.GetCommit(ctxOuter, testRepo, targetBranchName)
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startBranchCommit.Id})
}

func TestSuccessfulUserCommitFilesRequestStartSha(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)
	targetBranchName := "new"

	startCommit, err := log.GetCommit(ctxOuter, testRepo, "master")
	require.NoError(t, err)

	headerRequest := headerRequest(testRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
	setStartSha(headerRequest, startCommit.Id)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := log.GetCommit(ctxOuter, testRepo, targetBranchName)
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
}

func TestSuccessfulUserCommitFilesRequestStartShaRemoteRepository(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	newRepo, _, newRepoCleanupFn := testhelper.InitBareRepo(t)
	defer newRepoCleanupFn()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)
	targetBranchName := "new"

	startCommit, err := log.GetCommit(ctxOuter, testRepo, "master")
	require.NoError(t, err)

	headerRequest := headerRequest(newRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
	setStartSha(headerRequest, startCommit.Id)
	setStartRepository(headerRequest, testRepo)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := log.GetCommit(ctxOuter, newRepo, targetBranchName)
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
}

func TestSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	targetBranchName := "master"

	testCases := []struct {
		desc   string
		user   *gitalypb.User
		author *gitalypb.CommitAuthor // expected value
	}{
		{
			desc:   "special characters at start and end",
			user:   &gitalypb.User{Name: []byte(".,:;<>\"'\nJane Doe.,:;<>'\"\n"), Email: []byte(".,:;<>'\"\njanedoe@gitlab.com.,:;<>'\"\n"), GlId: testhelper.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
		{
			desc:   "special characters in the middle",
			user:   &gitalypb.User{Name: []byte("Ja<ne\n D>oe"), Email: []byte("ja<ne\ndoe>@gitlab.com"), GlId: testhelper.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := metadata.NewOutgoingContext(ctxOuter, md)
			headerRequest := headerRequest(testRepo, tc.user, targetBranchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, tc.user.Name, tc.user.Email)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))

			_, err = stream.CloseAndRecv()
			require.NoError(t, err)

			newCommit, err := log.GetCommit(ctxOuter, testRepo, targetBranchName)
			require.NoError(t, err)

			require.Equal(t, tc.author.Name, newCommit.Author.Name, "author name")
			require.Equal(t, tc.author.Email, newCommit.Author.Email, "author email")
			require.Equal(t, tc.author.Name, newCommit.Committer.Name, "committer name")
			require.Equal(t, tc.author.Email, newCommit.Committer.Email, "committer email")
		})
	}
}

func TestFailedUserCommitFilesRequestDueToHooks(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	branchName := "feature"
	filePath := "my/file.txt"
	headerRequest := headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage)
	actionsRequest1 := createFileHeaderRequest(filePath)
	actionsRequest2 := actionContentRequest("My content")
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			require.Contains(t, resp.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
			require.Contains(t, resp.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserCommitFilesRequestDueToIndexError(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
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
				headerRequest(testRepo, testhelper.TestUser, "feature", commitFilesMessage),
				createFileHeaderRequest("README.md"),
				actionContentRequest("This file already exists"),
			},
			indexError: "A file with this name already exists",
		},
		{
			desc: "file doesn't exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, testhelper.TestUser, "feature", commitFilesMessage),
				chmodFileHeaderRequest("documents/story.txt", true),
			},
			indexError: "A file with this name doesn't exist",
		},
		{
			desc: "dir already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, testhelper.TestUser, "utf-dir", commitFilesMessage),
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

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.indexError, resp.GetIndexError())
		})
	}
}

func TestFailedUserCommitFilesRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
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
			req:  headerRequest(nil, testhelper.TestUser, branchName, commitFilesMessage),
		},
		{
			desc: "empty User",
			req:  headerRequest(testRepo, nil, branchName, commitFilesMessage),
		},
		{
			desc: "empty BranchName",
			req:  headerRequest(testRepo, testhelper.TestUser, "", commitFilesMessage),
		},
		{
			desc: "empty CommitMessage",
			req:  headerRequest(testRepo, testhelper.TestUser, branchName, nil),
		},
		{
			desc: "invalid commit ID: \"foobar\"",
			req:  setStartSha(headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage), "foobar"),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(""), Email: []byte("")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(" "), Email: []byte(" ")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte("Jane Doe"), Email: []byte("")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(""), Email: []byte("janedoe@gitlab.com")}, branchName, commitFilesMessage),
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

func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage []byte) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Header{
			Header: &gitalypb.UserCommitFilesRequestHeader{
				Repository:      repo,
				User:            user,
				BranchName:      []byte(branchName),
				CommitMessage:   commitMessage,
				StartBranchName: nil,
				StartRepository: nil,
			},
		},
	}
}

func setAuthorAndEmail(headerRequest *gitalypb.UserCommitFilesRequest, authorName, authorEmail []byte) {
	header := getHeader(headerRequest)
	header.CommitAuthorName = authorName
	header.CommitAuthorEmail = authorEmail
}

func setStartBranchName(headerRequest *gitalypb.UserCommitFilesRequest, startBranchName []byte) {
	header := getHeader(headerRequest)
	header.StartBranchName = startBranchName
}

func setStartRepository(headerRequest *gitalypb.UserCommitFilesRequest, startRepository *gitalypb.Repository) {
	header := getHeader(headerRequest)
	header.StartRepository = startRepository
}

func setStartSha(headerRequest *gitalypb.UserCommitFilesRequest, startSha string) *gitalypb.UserCommitFilesRequest {
	header := getHeader(headerRequest)
	header.StartSha = startSha

	return headerRequest
}

func setForce(headerRequest *gitalypb.UserCommitFilesRequest, force bool) {
	header := getHeader(headerRequest)
	header.Force = force
}

func getHeader(headerRequest *gitalypb.UserCommitFilesRequest) *gitalypb.UserCommitFilesRequestHeader {
	return headerRequest.UserCommitFilesRequestPayload.(*gitalypb.UserCommitFilesRequest_Header).Header
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

func moveFileHeaderRequest(previousPath, filePath string, infer bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:       gitalypb.UserCommitFilesActionHeader_MOVE,
				FilePath:     []byte(filePath),
				PreviousPath: []byte(previousPath),
				InferContent: infer,
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
