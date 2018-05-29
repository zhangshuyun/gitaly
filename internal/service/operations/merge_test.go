package operations

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

var (
	mergeUser = &pb.User{
		Name:  []byte("Jane Doe"),
		Email: []byte("janedoe@example.com"),
		GlId:  "user-1",
	}

	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

func TestSuccessfulMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	prepareMergeBranch(t, testRepoPath)

	hooks := GitlabHooks
	hookTempfiles := make([]string, len(hooks))
	for i, h := range hooks {
		var hookPath string
		hookPath, hookTempfiles[i] = WriteEnvToHook(t, testRepoPath, h)
		defer os.Remove(hookPath)
		defer os.Remove(hookTempfiles[i])
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &pb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       mergeUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	_, err = gitlog.GetCommit(ctx, testRepo, firstResponse.CommitId, "")
	require.NoError(t, err, "look up git commit before merge is applied")

	require.NoError(t, mergeBidi.Send(&pb.UserMergeBranchRequest{Apply: true}), "apply merge")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")

	err = consumeEOF(func() error {
		_, err = mergeBidi.Recv()
		return err
	})
	require.NoError(t, err, "consume EOF")

	commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName, "")
	require.NoError(t, err, "look up git commit after call has finished")

	require.Equal(t, pb.OperationBranchUpdate{CommitId: commit.Id}, *(secondResponse.BranchUpdate))

	require.Contains(t, commit.ParentIds, mergeBranchHeadBefore, "merge parents must include previous HEAD of branch")
	require.Contains(t, commit.ParentIds, commitToMerge, "merge parents must include commit to merge")

	require.True(t, strings.HasPrefix(string(commit.Body), mergeCommitMessage), "expected %q to start with %q", commit.Body, mergeCommitMessage)

	author := commit.Author
	require.Equal(t, mergeUser.Name, author.Name)
	require.Equal(t, mergeUser.Email, author.Email)

	expectedGlID := "GL_ID=" + mergeUser.GlId
	for i, h := range hooks {
		hookEnv, err := ioutil.ReadFile(hookTempfiles[i])
		require.NoError(t, err)
		require.Contains(t, strings.Split(string(hookEnv), "\n"), expectedGlID, "expected env of hook %q to contain %q", h, expectedGlID)
	}
}

func TestAbortedMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	prepareMergeBranch(t, testRepoPath)

	firstRequest := &pb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       mergeUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("foobar"),
	}

	testCases := []struct {
		req       *pb.UserMergeBranchRequest
		closeSend bool
		desc      string
	}{
		{req: &pb.UserMergeBranchRequest{}, desc: "empty request, don't close"},
		{req: &pb.UserMergeBranchRequest{}, closeSend: true, desc: "empty request and close"},
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

			commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName, "")
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

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	prepareMergeBranch(t, testRepoPath)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &pb.UserMergeBranchRequest{
		Repository: testRepo,
		User:       mergeUser,
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

	require.NoError(t, mergeBidi.Send(&pb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	require.Equal(t, *secondResponse, pb.UserMergeBranchResponse{}, "response should be empty")

	commit, err := gitlog.GetCommit(ctx, testRepo, mergeBranchName, "")
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID, "RPC should not have trampled concurrent update")
}

func TestFailedMergeDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	prepareMergeBranch(t, testRepoPath)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			require.NoError(t, os.MkdirAll(path.Join(testRepoPath, "hooks"), 0755))
			hookPath := path.Join(testRepoPath, "hooks", hookName)
			ioutil.WriteFile(hookPath, hookContent, 0755)
			defer os.Remove(hookPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &pb.UserMergeBranchRequest{
				Repository: testRepo,
				User:       mergeUser,
				CommitId:   commitToMerge,
				Branch:     []byte(mergeBranchName),
				Message:    []byte(mergeCommitMessage),
			}

			require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

			_, err = mergeBidi.Recv()
			require.NoError(t, err, "receive first response")

			require.NoError(t, mergeBidi.Send(&pb.UserMergeBranchRequest{Apply: true}), "apply merge")
			require.NoError(t, mergeBidi.CloseSend(), "close send")

			secondResponse, err := mergeBidi.Recv()
			require.NoError(t, err, "receive second response")
			require.Contains(t, secondResponse.PreReceiveError, "failure")

			err = consumeEOF(func() error {
				_, err = mergeBidi.Recv()
				return err
			})
			require.NoError(t, err, "consume EOF")

			currentBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", mergeBranchName)
			require.Equal(t, mergeBranchHeadBefore, strings.TrimSpace(string(currentBranchHead)), "branch head updated")
		})
	}

}

func TestSuccessfulUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &pb.UserFFBranchRequest{
		Repository: testRepo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       mergeUser,
	}
	expectedResponse := &pb.UserFFBranchResponse{
		BranchUpdate: &pb.OperationBranchUpdate{
			RepoCreated:   false,
			BranchCreated: false,
			CommitId:      commitID,
		},
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchName).Run()

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	require.Equal(t, expectedResponse, resp)
	newBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", branchName)
	require.Equal(t, commitID, strings.TrimSpace(string(newBranchHead)), "branch head not updated")
}

func TestFailedUserFFBranchRequest(t *testing.T) {
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
		user     *pb.User
		branch   []byte
		commitID string
		repo     *pb.Repository
		code     codes.Code
	}{
		{
			desc:     "empty repository",
			user:     mergeUser,
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
			user:   mergeUser,
			branch: []byte(branchName),
			code:   codes.InvalidArgument,
		},
		{
			desc:     "non-existing commit",
			repo:     testRepo,
			user:     mergeUser,
			branch:   []byte(branchName),
			commitID: "f001",
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty branch",
			repo:     testRepo,
			user:     mergeUser,
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "non-existing branch",
			repo:     testRepo,
			user:     mergeUser,
			branch:   []byte("this-isnt-real"),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "commit is not a descendant of branch head",
			repo:     testRepo,
			user:     mergeUser,
			branch:   []byte(branchName),
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:     codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.UserFFBranchRequest{
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
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &pb.UserFFBranchRequest{
		Repository: testRepo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       mergeUser,
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")
	defer exec.Command("git", "-C", testRepoPath, "branch", "-d", branchName).Run()

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			hookPath := path.Join(testRepoPath, "hooks", hookName)
			ioutil.WriteFile(hookPath, hookContent, 0755)
			defer os.Remove(hookPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			resp, err := client.UserFFBranch(ctx, request)
			require.Nil(t, err)
			require.Contains(t, resp.PreReceiveError, "failure")
		})
	}
}

func prepareMergeBranch(t *testing.T, testRepoPath string) {
	deleteBranch(testRepoPath, mergeBranchName)
	out, err := exec.Command("git", "-C", testRepoPath, "branch", mergeBranchName, mergeBranchHeadBefore).CombinedOutput()
	require.NoError(t, err, "set up branch to merge into: %s", out)
}

func consumeEOF(errorFunc func() error) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- errorFunc()
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(1 * time.Second):
		err = fmt.Errorf("timed out waiting for EOF")
	}

	if err == io.EOF {
		err = nil
	}

	return err
}

func deleteBranch(testRepoPath, branchName string) {
	exec.Command("git", "-C", testRepoPath, "branch", "-D", branchName).Run()
}

// This error is used as a sentinel value
var errRecvTimeout = fmt.Errorf("timeout waiting for response")

func recvTimeout(bidi pb.OperationService_UserMergeBranchClient, timeout time.Duration) (*pb.UserMergeBranchResponse, error) {
	type responseError struct {
		response *pb.UserMergeBranchResponse
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
