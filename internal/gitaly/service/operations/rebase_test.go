package operations

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"fmt"
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
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	rebaseBranchName = "many_files"
)

func testSuccessfulUserRebaseConfirmableRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	pushOptions := []string{"ci.skip", "test=value"}
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, testhelper.GlID, "project-1", cfg, pushOptions...)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, repoPath, rebaseBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	preReceiveHookOutputPath, removePreReceive := gittest.WriteEnvToCustomHook(t, repoPath, "pre-receive")
	postReceiveHookOutputPath, removePostReceive := gittest.WriteEnvToCustomHook(t, repoPath, "post-receive")
	defer removePreReceive()
	defer removePostReceive()

	headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, "1", rebaseBranchName, branchSha, repoCopyProto, "master")
	headerRequest.GetHeader().GitPushOptions = pushOptions
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = rebaseStream.Recv()
		return err
	})

	newBranchSha := getBranchSha(t, repoPath, rebaseBranchName)

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

func testUserRebaseConfirmableStableCommitIDs(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, testhelper.GlID, "project-1", cfg)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	committerDate := &timestamp.Timestamp{Seconds: 100000000}
	parentSha := getBranchSha(t, repoPath, "master")

	require.NoError(t, rebaseStream.Send(&gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       repoProto,
				User:             testhelper.TestUser,
				RebaseId:         "1",
				Branch:           []byte(rebaseBranchName),
				BranchSha:        getBranchSha(t, repoPath, rebaseBranchName),
				RemoteRepository: repoProto,
				RemoteBranch:     []byte("master"),
				Timestamp:        committerDate,
			},
		},
	}), "send header")

	response, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")
	require.Equal(t, "c52b98024db0d3af0ccb20ed2a3a93a21cfbba87", response.GetRebaseSha())

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	response, err = rebaseStream.Recv()
	require.NoError(t, err, "receive second response")
	require.True(t, response.GetRebaseApplied())

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = rebaseStream.Recv()
		return err
	})

	commit, err := repo.ReadCommit(ctx, git.Revision(rebaseBranchName))
	require.NoError(t, err, "look up git commit")
	testhelper.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:   []byte("Add a directory with many files to allow testing of default 1,000 entry limit"),
		Body:      []byte("Add a directory with many files to allow testing of default 1,000 entry limit\n\nFor performance reasons, GitLab will add a file viewer limit and only show\nthe first 1,000 entries in a directory. Having this directory with many\nempty files in the test project will make the test easy.\n"),
		BodySize:  283,
		Id:        "c52b98024db0d3af0ccb20ed2a3a93a21cfbba87",
		ParentIds: []string{parentSha},
		TreeId:    "d0305132f880aa0ab4102e56a09cf1343ba34893",
		Author: &gitalypb.CommitAuthor{
			Name:  []byte("Drew Blessing"),
			Email: []byte("drew@gitlab.com"),
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     &timestamp.Timestamp{Seconds: 1510610637},
			Timezone: []byte("-0600"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     committerDate,
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func testFailedRebaseUserRebaseConfirmableRequestDueToInvalidHeader(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, repoPath, rebaseBranchName)

	testCases := []struct {
		desc string
		req  *gitalypb.UserRebaseConfirmableRequest
	}{
		{
			desc: "empty Repository",
			req:  buildHeaderRequest(nil, testhelper.TestUser, "1", rebaseBranchName, branchSha, repoCopy, "master"),
		},
		{
			desc: "empty User",
			req:  buildHeaderRequest(repo, nil, "1", rebaseBranchName, branchSha, repoCopy, "master"),
		},
		{
			desc: "empty Branch",
			req:  buildHeaderRequest(repo, testhelper.TestUser, "1", "", branchSha, repoCopy, "master"),
		},
		{
			desc: "empty BranchSha",
			req:  buildHeaderRequest(repo, testhelper.TestUser, "1", rebaseBranchName, "", repoCopy, "master"),
		},
		{
			desc: "empty RemoteRepository",
			req:  buildHeaderRequest(repo, testhelper.TestUser, "1", rebaseBranchName, branchSha, nil, "master"),
		},
		{
			desc: "empty RemoteBranch",
			req:  buildHeaderRequest(repo, testhelper.TestUser, "1", rebaseBranchName, branchSha, repoCopy, ""),
		},
		{
			desc: "invalid branch name",
			req:  buildHeaderRequest(repo, testhelper.TestUser, "1", rebaseBranchName, branchSha, repoCopy, "+dev:master"),
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

func testAbortedUserRebaseConfirmable(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, _, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

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
			testRepo, testRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "repo")
			defer cleanup()

			testRepoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
			defer cleanup()

			branchSha := getBranchSha(t, testRepoPath, rebaseBranchName)

			headerRequest := buildHeaderRequest(testRepo, testhelper.TestUser, fmt.Sprintf("%v", i), rebaseBranchName, branchSha, testRepoCopy, "master")

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

			secondResponse, err := rebaseRecvTimeout(rebaseStream, 1*time.Second)
			if err == errRecvTimeout {
				t.Fatal(err)
			}

			require.False(t, secondResponse.GetRebaseApplied(), "rebase should not have been applied")
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.code)

			newBranchSha := getBranchSha(t, testRepoPath, rebaseBranchName)
			require.Equal(t, newBranchSha, branchSha, "branch should not change when the rebase is aborted")
		})
	}
}

func testFailedUserRebaseConfirmableDueToApplyBeingFalse(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	testRepoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, repoPath, rebaseBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, "1", rebaseBranchName, branchSha, testRepoCopy, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(false)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.Error(t, err, "second response should have error")
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
	require.False(t, secondResponse.GetRebaseApplied(), "the second rebase is not applied")

	newBranchSha := getBranchSha(t, repoPath, rebaseBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase is not applied")
	require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase is not applied")
}

func testFailedUserRebaseConfirmableRequestDueToPreReceiveError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)
	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, repoPath, rebaseBranchName)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for i, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove := gittest.WriteCustomHook(t, repoPath, hookName, hookContent)
			defer remove()

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, fmt.Sprintf("%v", i), rebaseBranchName, branchSha, repoCopyProto, "master")
			require.NoError(t, rebaseStream.Send(headerRequest), "send header")

			firstResponse, err := rebaseStream.Recv()
			require.NoError(t, err, "receive first response")

			_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
			require.NoError(t, err, "look up git commit before rebase is applied")

			applyRequest := buildApplyRequest(true)
			require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

			secondResponse, err := rebaseStream.Recv()

			require.NoError(t, err, "receive second response")
			require.Contains(t, secondResponse.PreReceiveError, "failure")

			testhelper.ReceiveEOFWithTimeout(t, func() error {
				_, err = rebaseStream.Recv()
				return err
			})

			newBranchSha := getBranchSha(t, repoPath, rebaseBranchName)
			require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to PreReceiveError")
			require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase fails due to PreReceiveError")
		})
	}
}

func testFailedUserRebaseConfirmableDueToGitError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	failedBranchName := "rebase-encoding-failure-trigger"
	branchSha := getBranchSha(t, repoPath, failedBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, "1", failedBranchName, branchSha, repoCopyProto, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")
	require.Contains(t, firstResponse.GitError, "CONFLICT (content): Merge conflict in README.md")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = rebaseStream.Recv()
		return err
	})

	newBranchSha := getBranchSha(t, repoPath, failedBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to GitError")
}

func getBranchSha(t *testing.T, repoPath string, branchName string) string {
	branchSha := string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", branchName))
	return strings.TrimSpace(branchSha)
}

func testRebaseRequestWithDeletedFile(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, _, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)
	repoProto, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg.Storages[0])
	t.Cleanup(cleanup)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "copy")
	defer cleanup()

	branch := "rebase-delete-test"

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "user.name", string(testhelper.TestUser.Name))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "user.email", string(testhelper.TestUser.Email))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "checkout", "-b", branch, "master~1")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rm", "README")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-a", "-m", "delete file")

	branchSha := getBranchSha(t, repoPath, branch)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, "1", branch, branchSha, repoCopyProto, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = rebaseStream.Recv()
		return err
	})

	newBranchSha := getBranchSha(t, repoPath, branch)

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func testRebaseOntoRemoteBranch(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	remoteRepo, remoteRepoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg.Storages[0])
	defer cleanup()

	localBranch := "master"
	localBranchHash := getBranchSha(t, repoPath, localBranch)

	remoteBranch := "remote-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "config", "user.name", string(testhelper.TestUser.Name))
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "config", "user.email", string(testhelper.TestUser.Email))
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "checkout", "-b", remoteBranch, "master")
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rm", "README")
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "commit", "-a", "-m", "remove README")
	remoteBranchHash := getBranchSha(t, remoteRepoPath, remoteBranch)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	_, err = repo.ReadCommit(ctx, git.Revision(remoteBranchHash))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "remote commit does not yet exist in local repository")

	headerRequest := buildHeaderRequest(repoProto, testhelper.TestUser, "1", localBranch, localBranchHash, remoteRepo, remoteBranch)
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(remoteBranchHash))
	require.NoError(t, err, "remote commit does now exist in local repository")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	testhelper.ReceiveEOFWithTimeout(t, func() error {
		_, err = rebaseStream.Recv()
		return err
	})

	rebasedBranchHash := getBranchSha(t, repoPath, localBranch)

	require.NotEqual(t, rebasedBranchHash, localBranchHash)
	require.Equal(t, rebasedBranchHash, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func rebaseRecvTimeout(bidi gitalypb.OperationService_UserRebaseConfirmableClient, timeout time.Duration) (*gitalypb.UserRebaseConfirmableResponse, error) {
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
