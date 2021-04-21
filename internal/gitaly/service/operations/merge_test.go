package operations

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	commitToMerge         = "e63f41fe459e62e1228fcef60d7189127aeba95a"
	mergeBranchName       = "gitaly-merge-test-branch"
	mergeBranchHeadBefore = "281d3a76f31c812dbf48abce82ccf6860adedd81"
)

func testWithFeature(t *testing.T, feature featureflag.FeatureFlag, cfg config.Cfg, rubySrv *rubyserver.Server, testcase func(*testing.T, context.Context, config.Cfg, *rubyserver.Server)) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		feature,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testcase(t, ctx, cfg, rubySrv)
	})
}

func TestSuccessfulMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

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
		User:       testhelper.TestUser,
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

	require.Equal(t, gitalypb.OperationBranchUpdate{CommitId: commit.Id}, *(secondResponse.BranchUpdate))

	require.Equal(t, commit.ParentIds, []string{mergeBranchHeadBefore, commitToMerge})

	require.True(t, strings.HasPrefix(string(commit.Body), mergeCommitMessage), "expected %q to start with %q", commit.Body, mergeCommitMessage)

	author := commit.Author
	require.Equal(t, testhelper.TestUser.Name, author.Name)
	require.Equal(t, testhelper.TestUser.Email, author.Email)

	expectedGlID := "GL_ID=" + testhelper.TestUser.GlId
	for i, h := range hooks {
		hookEnv, err := ioutil.ReadFile(hookTempfiles[i])
		require.NoError(t, err)

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

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
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
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
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

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
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

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	mergeBidi, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	mergeCommitMessage := "Merged by Gitaly"
	firstRequest := &gitalypb.UserMergeBranchRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		CommitId:   commitToMerge,
		Branch:     []byte(mergeBranchName),
		Message:    []byte(mergeCommitMessage),
	}

	require.NoError(t, mergeBidi.Send(firstRequest), "send first request")
	firstResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive first response")

	// This concurrent update of the branch we are merging into should make the merge fail.
	concurrentCommitID := gittest.CreateCommit(t, cfg, repoPath, mergeBranchName, nil)
	require.NotEqual(t, firstResponse.CommitId, concurrentCommitID)

	require.NoError(t, mergeBidi.Send(&gitalypb.UserMergeBranchRequest{Apply: true}), "apply merge")
	require.NoError(t, mergeBidi.CloseSend(), "close send")

	secondResponse, err := mergeBidi.Recv()
	require.NoError(t, err, "receive second response")
	testhelper.ProtoEqual(t, secondResponse, &gitalypb.UserMergeBranchResponse{})

	commit, err := repo.ReadCommit(ctx, git.Revision(mergeBranchName))
	require.NoError(t, err, "get commit after RPC finished")
	require.Equal(t, commit.Id, concurrentCommitID, "RPC should not have trampled concurrent update")
}

func TestUserMergeBranch_ambiguousReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	merge, err := client.UserMergeBranch(ctx)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

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
		User:       testhelper.TestUser,
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

	require.Equal(t, gitalypb.OperationBranchUpdate{CommitId: commit.Id}, *(response.BranchUpdate))
	require.Equal(t, mergeCommitMessage, string(commit.Body))
	require.Equal(t, testhelper.TestUser.Name, commit.Author.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Author.Email)
	require.Equal(t, []string{mergeBranchHeadBefore, commitToMerge}, commit.ParentIds)
}

func TestFailedMergeDueToHooks(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			mergeBidi, err := client.UserMergeBranch(ctx)
			require.NoError(t, err)

			mergeCommitMessage := "Merged by Gitaly"
			firstRequest := &gitalypb.UserMergeBranchRequest{
				Repository: repo,
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

			testhelper.ReceiveEOFWithTimeout(t, func() error {
				_, err = mergeBidi.Recv()
				return err
			})

			currentBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", mergeBranchName)
			require.Equal(t, mergeBranchHeadBefore, text.ChompBytes(currentBranchHead), "branch head updated")
		})
	}
}

func TestSuccessfulUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
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

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}

func TestFailedUserFFBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

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
			repo:     repo,
			branch:   []byte(branchName),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:   "empty commit",
			repo:   repo,
			user:   testhelper.TestUser,
			branch: []byte(branchName),
			code:   codes.InvalidArgument,
		},
		{
			desc:     "non-existing commit",
			repo:     repo,
			user:     testhelper.TestUser,
			branch:   []byte(branchName),
			commitID: "f001",
			code:     codes.InvalidArgument,
		},
		{
			desc:     "empty branch",
			repo:     repo,
			user:     testhelper.TestUser,
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "non-existing branch",
			repo:     repo,
			user:     testhelper.TestUser,
			branch:   []byte("this-isnt-real"),
			commitID: commitID,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "commit is not a descendant of branch head",
			repo:     repo,
			user:     testhelper.TestUser,
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

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	branchName := "test-ff-target-branch"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
		CommitId:   commitID,
		Branch:     []byte(branchName),
		User:       testhelper.TestUser,
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", "-f", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f")

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

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

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
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath,
		"branch", branchName,
		"6d394385cf567f80a8fd85055db1ab4c5295806f")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "tag", branchName, "6d394385cf567f80a8fd85055db1ab4c5295806f~")

	commitID := "cfe32cf61b73a0d5e9f13e774abde7ff789b1660"
	request := &gitalypb.UserFFBranchRequest{
		Repository: repo,
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

	resp, err := client.UserFFBranch(ctx, request)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, expectedResponse, resp)
	newBranchHead := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "refs/heads/"+branchName)
	require.Equal(t, commitID, text.ChompBytes(newBranchHead), "branch head not updated")
}

func TestSuccessfulUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

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

func TestConflictsOnUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, "824be604a34828eb682305f0d963056cfac87b2d")

	request := &gitalypb.UserMergeToRefRequest{
		Repository:     repo,
		User:           testhelper.TestUser,
		TargetRef:      []byte("refs/merge-requests/x/written"),
		SourceSha:      "1450cd639e0bc6721eb02800169e464f212cde06",
		Message:        []byte("message1"),
		FirstParentRef: []byte("refs/heads/" + mergeBranchName),
	}

	t.Run("allow conflicts to be merged with markers", func(t *testing.T) {
		request.AllowConflicts = true

		resp, err := client.UserMergeToRef(ctx, request)
		require.NoError(t, err)

		var buf bytes.Buffer
		cmd := exec.Command(cfg.Git.BinPath, "-C", repoPath, "show", resp.CommitId)
		cmd.Stdout = &buf
		require.NoError(t, cmd.Run())

		bufStr := buf.String()
		require.Contains(t, bufStr, "+<<<<<<< files/ruby/popen.rb")
		require.Contains(t, bufStr, "+>>>>>>> files/ruby/popen.rb")
		require.Contains(t, bufStr, "+<<<<<<< files/ruby/regex.rb")
		require.Contains(t, bufStr, "+>>>>>>> files/ruby/regex.rb")
	})

	t.Run("disallow conflicts to be merged", func(t *testing.T) {
		request.AllowConflicts = false

		_, err := client.UserMergeToRef(ctx, request)
		require.Equal(t, status.Error(codes.FailedPrecondition, "Failed to create merge commit for source_sha 1450cd639e0bc6721eb02800169e464f212cde06 and target_sha 824be604a34828eb682305f0d963056cfac87b2d at refs/merge-requests/x/written"), err)
	})
}

func TestUserMergeToRef_stableMergeID(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	response, err := client.UserMergeToRef(ctx, &gitalypb.UserMergeToRefRequest{
		Repository:     repoProto,
		User:           testhelper.TestUser,
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
	require.Equal(t, &gitalypb.GitCommit{
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
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 12},
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func TestFailedUserMergeToRefRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

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
			repo:      repo,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty source SHA",
			repo:      repo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing commit",
			repo:      repo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			sourceSha: "f001",
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "empty branch and first parent ref",
			repo:      repo,
			user:      testhelper.TestUser,
			sourceSha: commitToMerge,
			targetRef: validTargetRef,
			code:      codes.InvalidArgument,
		},
		{
			desc:      "invalid target ref",
			repo:      repo,
			user:      testhelper.TestUser,
			branch:    []byte(branchName),
			sourceSha: commitToMerge,
			targetRef: []byte("refs/heads/branch"),
			code:      codes.InvalidArgument,
		},
		{
			desc:      "non-existing branch",
			repo:      repo,
			user:      testhelper.TestUser,
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

	ctx, _, repo, repoPath, client := setupOperationsService(t, ctx)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", mergeBranchName, mergeBranchHeadBefore)

	targetRef := []byte("refs/merge-requests/x/merge")
	mergeCommitMessage := "Merged by Gitaly"

	request := &gitalypb.UserMergeToRefRequest{
		Repository: repo,
		SourceSha:  commitToMerge,
		Branch:     []byte(mergeBranchName),
		TargetRef:  targetRef,
		User:       testhelper.TestUser,
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
