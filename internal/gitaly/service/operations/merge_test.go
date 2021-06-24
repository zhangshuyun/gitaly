package operations

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

func TestSuccessfulMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	hooks := GitlabHooks
	hookTempfiles := make([]string, len(hooks))
	for i, hook := range hooks {
		outputFile, err := ioutil.TempFile("", "")
		require.NoError(t, err)
		require.NoError(t, outputFile.Close())
		defer os.Remove(outputFile.Name())

		script := fmt.Sprintf("#!/bin/sh\n(cat && env) >%s \n", outputFile.Name())
		gittest.WriteCustomHook(t, repoPath, hook, []byte(script))

		hookTempfiles[i] = outputFile.Name()
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")

	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.CommitId))
	require.NoError(t, err, "look up git commit before merge is applied")

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = mergeBidi.Recv()
		return err
	})

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")

	testassert.ProtoEqual(t, &gitalypb.OperationBranchUpdate{CommitId: commit.Id}, secondResponse.BranchUpdate)

	require.Equal(t, commit.ParentIds, []string{mergeBranchHeadBefore, commitToMerge})

	require.True(t, strings.HasPrefix(string(commit.Body), mergeCommitMessage), "expected %q to start with %q", commit.Body, mergeCommitMessage)

	author := commit.Author
	require.Equal(t, gittest.TestUser.Name, author.Name)
	require.Equal(t, gittest.TestUser.Email, author.Email)

	expectedGlID := "GL_ID=" + gittest.TestUser.GlId
	for i, h := range hooks {
		hookEnv := testhelper.MustReadFile(t, hookTempfiles[i])

		lines := strings.Split(string(hookEnv), "\n")
		require.Contains(t, lines, expectedGlID, "expected env of hook %q to contain %q", h, expectedGlID)
		require.Contains(t, lines, "GL_PROTOCOL=web", "expected env of hook %q to contain GL_PROTOCOL")

		if h == "pre-receive" || h == "post-receive" {
			require.Regexp(t, mergeBranchHeadBefore+" .* refs/heads/"+mergeBranchName, lines[0], "expected env of hook %q to contain reference change", h)
		}
	}
}

func TestSuccessfulMerge_stableMergeIDs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte("Merged by Gitaly"),
		Timestamp:  &timestamp.Timestamp{Seconds: 12, Nanos: 34},
	}

	// Because the timestamp is
	expectedMergeID := "cd66941816adc76cc31fc6620d7b36a3dcb045e5"

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	response, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")
	require.Equal(t, response.CommitId, expectedMergeID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	response, err = mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	require.Equal(t, response.BranchUpdate.CommitId, expectedMergeID)

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = mergeBidi.Recv()
		return err
	})

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")
	require.Equal(t, commit, &gitalypb.GitCommit{
		Subject:  []byte("Merged by Gitaly"),
		Body:     []byte("Merged by Gitaly"),
		BodySize: 16,
		Id:       expectedMergeID,
		ParentIds: []string{
			"281d3a76f31c812dbf48abce82ccf6860adedd81",
			"e63f41fe459e62e1228fcef60d7189127aeba95a",
		},
		TreeId: "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
		Author: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
	})
}

func TestAbortedMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
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

			commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
			require.NoError(t, err, "look up git commit after call has finished")

			require.Equal(t, mergeBranchHeadBefore, commit.Id, "branch should not change when the merge is aborted")
		})
	}
}

func TestFailedMergeConcurrentUpdate(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	// This concurrent update of the branch we are merging into should make the merge fail.
	concurrentCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch(mergeBranchName))
	require.NotEqual(t, firstResponse.CommitId, concurrentCommitID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	testassert.ProtoEqual(t, secondResponse, &gitalypb.UserMergeBranchResponse{})

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID.String(), "RPC should not have trampled concurrent update")
}

func TestUserMergeBranch_ambiguousReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	merge, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	masterOID, err := repo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	// We're now creating all kinds of potentially ambiguous references in
	// the hope that UserMergeBranch won't be confused by it.
	for _, reference := range []string{
		mergeBranchName,
		"heads/" + mergeBranchName,
		"refs/heads/refs/heads/" + mergeBranchName,
		"refs/tags/" + mergeBranchName,
		"refs/tags/heads/" + mergeBranchName,
		"refs/tags/refs/heads/" + mergeBranchName,
	} {
		require.NoError(t, repo.UpdateRef(ctx, git.ReferenceName(reference), masterOID, git.ZeroOID))
	}

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, merge.Send(firstRequest), "send first request")

	_, err = merge.Recv()
	require.NoError(t, err, "receive first response")
	require.NoError(t, err, "look up git commit before merge is applied")
	require.NoError(t, merge.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")

	response, err := merge.Recv()
	require.NoError(t, err, "receive second response")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = merge.Recv()
		return err
	})

	commit, err := repo.ReadCommit(ctx, git.Revision("refs/heads/"+mergeBranchName))
	require.NoError(t, err, "look up git commit after call has finished")

	testassert.ProtoEqual(t, &gitalypb.OperationBranchUpdate{CommitId: commit.Id}, response.BranchUpdate)
	require.Equal(t, mergeCommitMessage, string(commit.Body))
	require.Equal(t, gittest.TestUser.Name, commit.Author.Name)
	require.Equal(t, gittest.TestUser.Email, commit.Author.Email)
	require.Equal(t, []string{mergeBranchHeadBefore, commitToMerge}, commit.ParentIds)
}

func TestFailedMergeDueToHooks(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &gitalypb.UserMergeBranchRequest{
				Repository: repo,
				User:       gittest.TestUser,
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

			testhelper.ReceiveEOFWithTimeout(t, func() error {
				_, err = mergeBidi.Recv()
				return err
			})

			currentBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", mergeBranchName)
			require.Equal(t, mergeBranchHeadBefore, text.ChompBytes(currentBranchHead), "branch head updated")
		})
	}
}

func TestSuccessfulUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       gittest.TestUser,
	}
	expectedResponse := &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			RepoCreated:   false,
			BranchCreated: false,
			CommitId:      commitID,
		},
	}

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testassert.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}

func TestFailedUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

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
			user:     gittest.TestUser,
			branch:   []byte(branchName),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty user",
			repo:     repo,
			branch:   []byte(branchName),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:   "empty commit",
			repo:   repo,
			user:   gittest.TestUser,
			branch: []byte(branchName),
			code:   codes.InvalidArgument,
		},
		{
			desc:     "non-existing commit",
			repo:     repo,
			user:     gittest.TestUser,
			branch:   []byte(branchName),
			commitID: "f001",
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty branch",
			repo:     repo,
			user:     gittest.TestUser,
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "non-existing branch",
			repo:     repo,
			user:     gittest.TestUser,
			branch:   []byte("this-isnt-real"),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "commit is not a descendant of branch head",
			repo:     repo,
			user:     gittest.TestUser,
			branch:   []byte(branchName),
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:     codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
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
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       gittest.TestUser,
	}

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			resp, err := client.UserFFBranch(ctx, request)
			require.Nil(t, err)
			require.Contains(t, resp.PreReceiveError, "failure")
		})
	}
}

func TestUserFFBranch_ambiguousReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	branchName := "test-ff-target-branch"

	// We're creating both a branch and a tag with the same name.
	// If `git rev-parse` is called on the branch name directly
	// without using the fully qualified reference, then it would
	// return the OID of the tag instead of the branch.
	//
	// In the past, this used to cause us to use the tag's OID as
	// old revision when calling git-update-ref. As a result, the
	// update would've failed as the branch's current revision
	// didn't match the specified old revision.
	gittest.Exec(t, cfg, "-C", repoPath,
		"branch", branchName,
		"6d394385cf567f80a8fd85055db1ab4c5295806f")
	gittest.Exec(t, cfg, "-C", repoPath, "tag", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f~")

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       gittest.TestUser,
	}
	expectedResponse := &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			RepoCreated:   false,
			BranchCreated: false,
			CommitId:      commitID,
		},
	}

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testassert.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/"+branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}

func TestSuccessfulUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	existingTargetRef := []byte("refs/merge-requests/x/written")
	emptyTargetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	// Writes in existingTargetRef
	beforeRefreshCommitSha := "a5391128b0ef5d21df5dd23d98557f4ef12fae20"
	out, err := exec.Command(cfg.Git.BinPath, "-C", repoPath, "update-ref", string(existingTargetRef), beforeRefreshCommitSha).CombinedOutput()
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
			user:           gittest.TestUser,
			targetRef:      emptyTargetRef,
			emptyRef:       true,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:           "existing target ref",
			user:           gittest.TestUser,
			targetRef:      existingTargetRef,
			emptyRef:       false,
			sourceSha:      commitToMerge,
			message:        mergeCommitMessage,
			firstParentRef: []byte("refs/heads/" + mergeBranchName),
		},
		{
			desc:      "branch is specified and firstParentRef is empty",
			user:      gittest.TestUser,
			branch:    []byte(mergeBranchName),
			targetRef: existingTargetRef,
			emptyRef:  false,
			sourceSha: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
			message:   mergeCommitMessage,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserMergeToRefRequest{
				Repository:     repoProto,
				User:           testCase.user,
				Branch:         testCase.branch,
				TargetRef:      testCase.targetRef,
				SourceSha:      testCase.sourceSha,
				Message:        []byte(testCase.message),
				FirstParentRef: testCase.firstParentRef,
			}

			commitBeforeRefMerge, fetchRefBeforeMergeErr := repo.ReadCommit(ctx, git.Revision(testCase.targetRef))
			if testCase.emptyRef {
				require.Error(t, fetchRefBeforeMergeErr, "error when fetching empty ref commit")
			} else {
				require.NoError(t, fetchRefBeforeMergeErr, "no error when fetching existing ref")
			}

			resp, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)

			commit, err := repo.ReadCommit(ctx, git.Revision(testCase.targetRef))
			require.NoError(t, err, "look up git commit after call has finished")

			// Asserts commit parent SHAs
			require.Equal(t, []string{mergeBranchHeadBefore, testCase.sourceSha}, commit.ParentIds, "merge commit parents must be the sha before HEAD and source sha")

			require.True(t, strings.HasPrefix(string(commit.Body), testCase.message), "expected %q to start with %q", commit.Body, testCase.message)

			// Asserts author
			author := commit.Author
			require.Equal(t, gittest.TestUser.Name, author.Name)
			require.Equal(t, gittest.TestUser.Email, author.Email)

			require.Equal(t, resp.CommitId, commit.Id)

			// Calling commitBeforeRefMerge.Id in a non-existent
			// commit will raise a null-pointer error.
			if !testCase.emptyRef {
				require.NotEqual(t, commit.Id, commitBeforeRefMerge.Id)
			}
		})
	}
}

func TestConflictsOnUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	t.Run("allow conflicts to be merged with markers when modified on both sides", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", "modified-both-sides-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< files\/ruby\/popen.rb.*?\+>>>>>>> files\/ruby\/popen.rb.*?\+<<<<<<< files\/ruby\/regex.rb.*?\+>>>>>>> files\/ruby\/regex.rb`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when modified on source and removed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "eb227b3e214624708c474bdab7bde7afc17cefcc", "92417abf83b75e67b8ace920bc8e83e1986da4ac", "modified-source-removed-target-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< \n.*?\+=======\n.*?\+>>>>>>> files/ruby/version_info.rb`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when removed on source and modified on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "92417abf83b75e67b8ace920bc8e83e1986da4ac", "eb227b3e214624708c474bdab7bde7afc17cefcc", "removed-source-modified-target-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< files/ruby/version_info.rb.*?\+=======\n.*?\+>>>>>>> \z`)
		require.Regexp(t, markersRegexp, output)
	})

	t.Run("allow conflicts to be merged with markers when both source and target added the same file", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "f0f390655872bb2772c85a0128b2fbc2d88670cb", "5b4bb08538b9249995b94aa69121365ba9d28082", "source-target-added-same-file-conflict")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		markersRegexp := regexp.MustCompile(`(?s)\+<<<<<<< NEW_FILE.md.*?\+=======\n.*?\+>>>>>>> NEW_FILE.md`)
		require.Regexp(t, markersRegexp, output)
	})

	// Test cases below do not show any conflict markers because we don't try
	// to merge the conflicts for these cases. We keep `Their` side of the
	// conflict and ignore `Our` and `Ancestor` instead. This is because we want
	// to show the `Their` side when we present the merge commit on the merge
	// request diff.
	t.Run("allow conflicts to be merged without markers when renamed on source and removed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "3ac7abfb7621914e596d5bf369be8234b9086052", "renamed-source-removed-target")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("allow conflicts to be merged without markers when removed on source and renamed on target", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "3ac7abfb7621914e596d5bf369be8234b9086052", "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "removed-source-renamed-target")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("allow conflicts to be merged without markers when both source and target renamed the same file", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "aafecf84d791ec43dfa16e55eb0a0fbd9c72d3fb", "fe6d6ff5812e7fb292168851dc0edfc6a0171909", "source-target-renamed-same-file")
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		output := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "show", resp.CommitId))

		require.NotContains(t, output, "=======")
	})

	t.Run("disallow conflicts to be merged", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", "disallowed-conflicts")
		request.AllowConflicts = false

		_, err := client.UserMergeToRef(ctx, request)
		testassert.GrpcEqualErr(t, status.Error(codes.FailedPrecondition, "Failed to create merge commit for source_sha 1450cd639e0bc6721eb02800169e464f212cde06 and target_sha 824be604a34828eb682305f0d963056cfac87b2d at refs/merge-requests/x/written"), err)
	})

	targetRef := git.Revision("refs/merge-requests/foo")

	t.Run("failing merge does not update target reference if skipping precursor update-ref", func(t *testing.T) {
		request := buildUserMergeToRefRequest(t, cfg, repoProto, repoPath, "1450cd639e0bc6721eb02800169e464f212cde06", "824be604a34828eb682305f0d963056cfac87b2d", t.Name())
		request.TargetRef = []byte(targetRef)

		_, err := client.UserMergeToRef(ctx, request)
		testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)

		hasRevision, err := repo.HasRevision(ctx, targetRef)
		require.NoError(t, err)
		require.False(t, hasRevision, "branch should not have been created")
	})
}

func buildUserMergeToRefRequest(t testing.TB, cfg config.Cfg, repo *gitalypb.Repository, repoPath string, sourceSha string, targetSha string, mergeBranchName string) *gitalypb.UserMergeToRefRequest {
	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, targetSha)

	return &gitalypb.UserMergeToRefRequest{
		Repository:     repo,
		User:           gittest.TestUser,
		TargetRef:      []byte("refs/merge-requests/x/written"),
		SourceSha:      sourceSha,
		Message:        []byte("message1"),
		FirstParentRef: []byte("refs/heads/" + mergeBranchName),
	}
}

func TestUserMergeToRef_stableMergeID(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	response, err := client.UserMergeToRef(ctx, &gitalypb.UserMergeToRefRequest{
		Repository:     repoProto,
		User:           gittest.TestUser,
		FirstParentRef: []byte("refs/heads/" + mergeBranchName),
		TargetRef:      []byte("refs/merge-requests/x/written"),
		SourceSha:      "1450cd639e0bc6721eb02800169e464f212cde06",
		Message:        []byte("Merge message"),
		Timestamp:      &timestamp.Timestamp{Seconds: 12, Nanos: 34},
	})
	require.NoError(t, err)
	require.Equal(t, "a04514f4e6b4e272989b39cca1ebdbb670abdfd6", response.CommitId)

	commit, err := repo.ReadCommit(ctx, git.Revision("refs/merge-requests/x/written"))
	require.NoError(t, err, "look up git commit after call has finished")
	testassert.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:  []byte("Merge message"),
		Body:     []byte("Merge message"),
		BodySize: 13,
		Id:       "a04514f4e6b4e272989b39cca1ebdbb670abdfd6",
		ParentIds: []string{
			"281d3a76f31c812dbf48abce82ccf6860adedd81",
			"1450cd639e0bc6721eb02800169e464f212cde06",
		},
		TreeId: "3d3c2dd807abaf36d7bd5334bf3f8c5cf61bad75",
		Author: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func TestFailedUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

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
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty user",
			repo:      repo,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty source SHA",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing commit",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: "f001",
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty branch and first parent ref",
			repo:      repo,
			user:      gittest.TestUser,
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "invalid target ref",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: []byte("refs/heads/branch"),
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing branch",
			repo:      repo,
			user:      gittest.TestUser,
			branch:    []byte("this-isnt-real"),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
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
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	gittest.Exec(t, cfg, "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	targetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	request := &gitalypb.UserMergeToRefRequest{
		Repository: repo,
		SourceSha:  commitToMerge,
		Branch:     []byte(mergeBranchName),
		TargetRef:  targetRef,
		User:       gittest.TestUser,
		Message:    []byte(mergeCommitMessage),
	}

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			resp, err := client.UserMergeToRef(ctx, request)
			require.NoError(t, err)
			require.Empty(t, resp.PreReceiveError)
		})
	}
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
