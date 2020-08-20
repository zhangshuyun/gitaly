package operations

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

func TestSuccessfulMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testSuccessfulMerge(t, ctx)
}

func testSuccessfulMerge(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	prepareMergeBranch(t, testRepoPath)

	hooks := GitlabHooks
	hookTempfiles := make([]string, len(hooks))
	for i, h := range hooks {
		var cleanup func()
		hookTempfiles[i], cleanup = testhelper.WriteEnvToCustomHook(t, testRepoPath, h)
		defer cleanup()
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       testhelper.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	_, err = gitlog.GetCommit(ctx, testRepo, firstResponse.CommitId)
	require.NoError(t, err, "look up git commit before merge is applied")

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")

	err = testhelper.ReceiveEOFWithTimeout(func() error {
		_, err = mergeBidi.Recv()
		return err
	})
	require.NoError(t, err, "consume EOF")

	commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName)
	require.NoError(t, err, "look up git commit after call has finished")

	require.Equal(t, gitalypb.OperationBranchUpdate{CommitId: commit.Id}, *(secondResponse.BranchUpdate))

	require.Contains(t, commit.ParentIds, mergeBranchHeadBefore, "merge parents must include previous HEAD of branch")
	require.Contains(t, commit.ParentIds, commitToMerge, "merge parents must include commit to merge")

	require.True(t, strings.HasPrefix(string(commit.Body), mergeCommitMessage), "expected %q to start with %q", commit.Body, mergeCommitMessage)

	author := commit.Author
	require.Equal(t, testhelper.TestUser.Name, author.Name)
	require.Equal(t, testhelper.TestUser.Email, author.Email)

	expectedGlID := "GL_ID=" + testhelper.TestUser.GlId
	for i, h := range hooks {
		hookEnv, err := ioutil.ReadFile(hookTempfiles[i])
		require.NoError(t, err)
		require.Contains(t, strings.Split(string(hookEnv), "\n"), expectedGlID, "expected env of hook %q to contain %q", h, expectedGlID)
	}
}

func TestAbortedMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	prepareMergeBranch(t, testRepoPath)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       testhelper.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("foobar"),
	}

	testCases := []struct {
		req       *gitalypb.UserMergeBranchRequest
		closeSend bool
		desc      string
	}{
		{req: &gitalypb.UserMergeBranchRequest{}, desc: "empty request, don't close"},
		{req: &gitalypb.UserMergeBranchRequest{}, closeSend: true, desc: "empty request and close"},
		{closeSend: true, desc: "no request just close"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			firstResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "first response")
			require.NotEqual(t, "", firstResponse.CommitId, "commit ID on first response")

			if tc.req != nil {
				require.NoError(t, mergeBidi.Send(tc.req), "send second request")
			}

			if tc.closeSend {
				require.NoError(t, mergeBidi.CloseSend(), "close request stream from client")
			}

			secondResponse, err := recvTimeout(mergeBidi, 1*time.Second)
			if err == errRecvTimeout {
				t.Fatal(err)
			}

			require.Equal(t, "", secondResponse.GetBranchUpdate().GetCommitId(), "merge should not have been applied")
			require.Error(t, err)

			commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName)
			require.NoError(t, err, "look up git commit after call has finished")

			require.Equal(t, mergeBranchHeadBefore, commit.Id, "branch should not change when the merge is aborted")
		})
	}
}

func TestFailedMergeConcurrentUpdate(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	prepareMergeBranch(t, testRepoPath)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       testhelper.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	// This concurrent update of the branch we are merging into should make the merge fail.
	concurrentCommitID := testhelper.CreateCommit(t, testRepoPath, mergeBranchName, nil)
	require.NotEqual(t, firstResponse.CommitId, concurrentCommitID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	testhelper.ProtoEqual(t, secondResponse, &gitalypb.UserMergeBranchResponse{})

	commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName)
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID, "RPC should not have trampled concurrent update")
}

func TestFailedMergeDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	prepareMergeBranch(t, testRepoPath)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			ctx, cancel := testhelper.Context()
			defer cancel()

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &gitalypb.UserMergeBranchRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte(mergeCommitMessage),
			}

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			_, err = mergeBidi.Recv()
			require.NoError(t, err, "receive first response")

			require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
			require.NoError(t, mergeBidi.CloseSend(), "close send")

			secondResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "receive second response")
			require.Contains(t, secondResponse.PreReceiveError, "failure")

			err = testhelper.ReceiveEOFWithTimeout(func() error {
				_, err = mergeBidi.Recv()
				return err
			})
			require.NoError(t, err, "consume EOF")

			currentBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", mergeBranchName)
			require.Equal(t, mergeBranchHeadBefore, text.ChompBytes(currentBranchHead), "branch head updated")
		})
	}
}

func TestSuccessfulUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: testRepo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       testhelper.TestUser,
	}
	expectedResponse := &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			RepoCreated:   false,
			BranchCreated: false,
			CommitId:      commitID,
		},
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchName).Run()

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}

func TestFailedUserFFBranchRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchName).Run()

	testCases := []struct {
		desc     string
		user     *gitalypb.User
		branch   []byte
		commitID string
		repo     *gitalypb.Repository
		code     codes.Code
	}{
		{
			desc:     "empty repository",
			user:     testhelper.TestUser,
			branch:   []byte(branchName),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty user",
			repo:     testRepo,
			branch:   []byte(branchName),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:   "empty commit",
			repo:   testRepo,
			user:   testhelper.TestUser,
			branch: []byte(branchName),
			code:   codes.InvalidArgument,
		},
		{
			desc:     "non-existing commit",
			repo:     testRepo,
			user:     testhelper.TestUser,
			branch:   []byte(branchName),
			commitID: "f001",
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty branch",
			repo:     testRepo,
			user:     testhelper.TestUser,
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "non-existing branch",
			repo:     testRepo,
			user:     testhelper.TestUser,
			branch:   []byte("this-isnt-real"),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "commit is not a descendant of branch head",
			repo:     testRepo,
			user:     testhelper.TestUser,
			branch:   []byte(branchName),
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:     codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.UserFFBranchRequest{
				Repository: testCase.repo,
				User:       testCase.user,
				Branch:     testCase.branch,
				CommitId:   testCase.commitID,
			}
			_, err := client.UserFFBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFailedUserFFBranchDueToHooks(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: testRepo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       testhelper.TestUser,
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchName).Run()

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			ctx, cancel := testhelper.Context()
			defer cancel()

			resp, err := client.UserFFBranch(ctx, request)
			require.Nil(t, err)
			require.Contains(t, resp.PreReceiveError, "failure")
		})
	}
}

func TestSuccessfulUserMergeToRefRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	prepareMergeBranch(t, testRepoPath)

	existingTargetRef := []byte("refs/merge-requests/x/written")
	emptyTargetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	// Writes in existingTargetRef
	beforeRefreshCommitSha := "a5391128b0ef5d21df5dd23d98557f4ef12fae20"
	out, err := exec.Command("git", "-C", testRepoPath, "update-ref", string(existingTargetRef), beforeRefreshCommitSha).CombinedOutput()
	require.NoError(t, err, "give an existing state to the target ref: %s", out)

	testCases := []struct {
		desc           string
		user           *gitalypb.User
		branch         []byte
		targetRef      []byte
		emptyRef       bool
		sourceSha      string
		message        string
		firstParentRef []byte
	}{
		{
			desc:           "empty target ref merge",
			user:           testhelper.TestUser,
			targetRef:      emptyTargetRef,
			emptyRef:       true,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:           "existing target ref",
			user:           testhelper.TestUser,
			targetRef:      existingTargetRef,
			emptyRef:       false,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:      "branch is specified and firstParentRef is empty",
			user:      testhelper.TestUser,
			branch:    []byte(mergeBranchName),
			targetRef: existingTargetRef,
			emptyRef:  false,
			sourceSha: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
			message:   mergeCommitMessage,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.UserMergeToRefRequest{
				Repository:     testRepo,
				User:           testCase.user,
				Branch:         testCase.branch,
				TargetRef:      testCase.targetRef,
				SourceSha:      testCase.sourceSha,
				Message:        []byte(testCase.message),
				FirstParentRef: testCase.firstParentRef,
			}

			commitBeforeRefMerge, fetchRefBeforeMergeErr := gitlog.GetCommit(ctx, testRepo, string(testCase.targetRef))
			if testCase.emptyRef {
				require.Error(t, fetchRefBeforeMergeErr, "error when fetching empty ref commit")
			} else {
				require.NoError(t, fetchRefBeforeMergeErr, "no error when fetching existing ref")
			}

			resp, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)

			commit, err := gitlog.GetCommit(ctx, testRepo, string(testCase.targetRef))
			require.NoError(t, err, "look up git commit after call has finished")

			// Asserts commit parent SHAs
			require.Equal(t, []string{mergeBranchHeadBefore, testCase.sourceSha}, commit.ParentIds, "merge commit parents must be the sha before HEAD and source sha")

			require.True(t, strings.HasPrefix(string(commit.Body), testCase.message), "expected %q to start with %q", commit.Body, testCase.message)

			// Asserts author
			author := commit.Author
			require.Equal(t, testhelper.TestUser.Name, author.Name)
			require.Equal(t, testhelper.TestUser.Email, author.Email)

			require.Equal(t, resp.CommitId, commit.Id)

			// Calling commitBeforeRefMerge.Id in a non-existent
			// commit will raise a null-pointer error.
			if !testCase.emptyRef {
				require.NotEqual(t, commit.Id, commitBeforeRefMerge.Id)
			}
		})
	}
}

func TestFailedUserMergeToRefRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	prepareMergeBranch(t, testRepoPath)

	validTargetRef := []byte("refs/merge-requests/x/merge")

	testCases := []struct {
		desc      string
		user      *gitalypb.User
		branch    []byte
		targetRef []byte
		sourceSha string
		repo      *gitalypb.Repository
		code      codes.Code
	}{
		{
			desc:      "empty repository",
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty user",
			repo:      testRepo,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty source SHA",
			repo:      testRepo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing commit",
			repo:      testRepo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			sourceSha: "f001",
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty branch and first parent ref",
			repo:      testRepo,
			user:      testhelper.TestUser,
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "invalid target ref",
			repo:      testRepo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: []byte("refs/heads/branch"),
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing branch",
			repo:      testRepo,
			user:      testhelper.TestUser,
			branch:    []byte("this-isnt-real"),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.UserMergeToRefRequest{
				Repository: testCase.repo,
				User:       testCase.user,
				Branch:     testCase.branch,
				SourceSha:  testCase.sourceSha,
				TargetRef:  testCase.targetRef,
			}
			_, err := client.UserMergeToRef(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestUserMergeToRefIgnoreHooksRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	prepareMergeBranch(t, testRepoPath)

	targetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	request := &gitalypb.UserMergeToRefRequest{
		Repository: testRepo,
		SourceSha:  commitToMerge,
		Branch:     []byte(mergeBranchName),
		TargetRef:  targetRef,
		User:       testhelper.TestUser,
		Message:    []byte(mergeCommitMessage),
	}

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			ctx, cancel := testhelper.Context()
			defer cancel()

			resp, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)
			require.Empty(t, resp.PreReceiveError)
		})
	}
}

func prepareMergeBranch(t *testing.T, testRepoPath string) {
	deleteBranch(testRepoPath, mergeBranchName)
	out, err := exec.Command("git", "-C", testRepoPath, "branch", mergeBranchName, mergeBranchHeadBefore).CombinedOutput()
	require.NoError(t, err, "set up branch to merge into: %s", out)
}

func deleteBranch(testRepoPath, branchName string) {
	exec.Command("git", "-C", testRepoPath, "branch", "-D", branchName).Run()
}

// This error is used as a sentinel value
var errRecvTimeout = fmt.Errorf("timeout waiting for response")

func recvTimeout(bidi gitalypb.OperationService_UserMergeBranchClient, timeout time.Duration) (*gitalypb.UserMergeBranchResponse, error) {
	type responseError struct {
		response *gitalypb.UserMergeBranchResponse
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
