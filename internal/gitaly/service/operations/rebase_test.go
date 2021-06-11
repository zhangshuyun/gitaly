package operations

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

var (
	rebaseBranchName = "many_files"
)

func TestSuccessfulUserRebaseConfirmableRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	pushOptions := []string{"ci.skip", "test=value"}
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg, pushOptions...)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	preReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "pre-receive")
	postReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "post-receive")

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", rebaseBranchName, branchSha, repoCopyProto, "master")
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

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	newBranchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

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

func TestUserRebaseConfirmableTransaction(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	var voteCount int
	txManager := &transaction.MockManager{
		VoteFn: func(context.Context, txinfo.Transaction, txinfo.PraefectServer, voting.Vote) error {
			voteCount++
			return nil
		},
	}

	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(
		t, ctx, testcfg.Build(t), nil,
		// Praefect would intercept our call and inject its own transaction.
		testserver.WithDisablePraefect(),
		testserver.WithTransactionManager(txManager),
	)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc                 string
		withTransaction      bool
		primary              bool
		expectedVotes        int
		expectPreReceiveHook bool
	}{
		{
			desc:                 "non-transactonal does not vote but executes hook",
			expectedVotes:        0,
			expectPreReceiveHook: true,
		},
		{
			desc:                 "primary votes and executes hook",
			withTransaction:      true,
			primary:              true,
			expectedVotes:        2,
			expectPreReceiveHook: true,
		},
		{
			desc:                 "secondary votes but does not execute hook",
			withTransaction:      true,
			primary:              false,
			expectedVotes:        2,
			expectPreReceiveHook: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			preReceiveHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "pre-receive")

			voteCount = 0

			ctx := ctx
			if tc.withTransaction {
				ctx = helper.OutgoingToIncoming(ctx)

				var err error
				ctx, err = txinfo.InjectTransaction(ctx, 1, "node", tc.primary)
				require.NoError(t, err)
				ctx, err = (&txinfo.PraefectServer{
					SocketPath: "irrelevant",
				}).Inject(ctx)
				require.NoError(t, err)

				ctx = helper.IncomingToOutgoing(ctx)
			}

			branchSha, err := repo.ResolveRevision(ctx, git.Revision(rebaseBranchName))
			require.NoError(t, err)

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", rebaseBranchName, branchSha.String(), repoProto, "master")
			require.NoError(t, rebaseStream.Send(headerRequest))
			_, err = rebaseStream.Recv()
			require.NoError(t, err)

			require.NoError(t, rebaseStream.Send(buildApplyRequest(true)), "apply rebase")
			secondResponse, err := rebaseStream.Recv()
			require.NoError(t, err)
			require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")

			response, err := rebaseStream.Recv()
			require.Nil(t, response)
			require.Equal(t, io.EOF, err)

			require.Equal(t, tc.expectedVotes, voteCount)
			if tc.expectPreReceiveHook {
				require.FileExists(t, preReceiveHookOutputPath)
			} else {
				require.NoFileExists(t, preReceiveHookOutputPath)
			}
		})
	}
}

func TestUserRebaseConfirmableStableCommitIDs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	committerDate := &timestamp.Timestamp{Seconds: 100000000}
	parentSha := getBranchSha(t, cfg, repoPath, "master")

	require.NoError(t, rebaseStream.Send(&gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository:       repoProto,
				User:             gittest.TestUser,
				RebaseId:         "1",
				Branch:           []byte(rebaseBranchName),
				BranchSha:        getBranchSha(t, cfg, repoPath, rebaseBranchName),
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

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	commit, err := repo.ReadCommit(ctx, git.Revision(rebaseBranchName))
	require.NoError(t, err, "look up git commit")
	testassert.ProtoEqual(t, &gitalypb.GitCommit{
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
			Name:  gittest.TestUser.Name,
			Email: gittest.TestUser.Email,
			// Nanoseconds get ignored because commit timestamps aren't that granular.
			Date:     committerDate,
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func TestFailedRebaseUserRebaseConfirmableRequestDueToInvalidHeader(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	repoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	testCases := []struct {
		desc string
		req  *gitalypb.UserRebaseConfirmableRequest
	}{
		{
			desc: "empty Repository",
			req:  buildHeaderRequest(nil, gittest.TestUser, "1", rebaseBranchName, branchSha, repoCopy, "master"),
		},
		{
			desc: "empty User",
			req:  buildHeaderRequest(repo, nil, "1", rebaseBranchName, branchSha, repoCopy, "master"),
		},
		{
			desc: "empty Branch",
			req:  buildHeaderRequest(repo, gittest.TestUser, "1", "", branchSha, repoCopy, "master"),
		},
		{
			desc: "empty BranchSha",
			req:  buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, "", repoCopy, "master"),
		},
		{
			desc: "empty RemoteRepository",
			req:  buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, branchSha, nil, "master"),
		},
		{
			desc: "empty RemoteBranch",
			req:  buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, branchSha, repoCopy, ""),
		},
		{
			desc: "invalid branch name",
			req:  buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, branchSha, repoCopy, "+dev:master"),
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
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

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
			testRepo, testRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "repo")
			defer cleanup()

			testRepoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
			defer cleanup()

			branchSha := getBranchSha(t, cfg, testRepoPath, rebaseBranchName)

			headerRequest := buildHeaderRequest(testRepo, gittest.TestUser, fmt.Sprintf("%v", i), rebaseBranchName, branchSha, testRepoCopy, "master")

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

			newBranchSha := getBranchSha(t, cfg, testRepoPath, rebaseBranchName)
			require.Equal(t, newBranchSha, branchSha, "branch should not change when the rebase is aborted")
		})
	}
}

func TestFailedUserRebaseConfirmableDueToApplyBeingFalse(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testRepoCopy, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", rebaseBranchName, branchSha, testRepoCopy, "master")
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

	newBranchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase is not applied")
	require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase is not applied")
}

func TestFailedUserRebaseConfirmableRequestDueToPreReceiveError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	hookContent := []byte("#!/bin/sh\necho 'failure'\nexit 1")

	for i, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, fmt.Sprintf("%v", i), rebaseBranchName, branchSha, repoCopyProto, "master")
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

			_, err = rebaseStream.Recv()
			require.Equal(t, io.EOF, err)

			newBranchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)
			require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to PreReceiveError")
			require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase fails due to PreReceiveError")
		})
	}
}

func TestFailedUserRebaseConfirmableDueToGitError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	failedBranchName := "rebase-encoding-failure-trigger"
	branchSha := getBranchSha(t, cfg, repoPath, failedBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", failedBranchName, branchSha, repoCopyProto, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")
	require.Contains(t, firstResponse.GitError, "conflict")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	newBranchSha := getBranchSha(t, cfg, repoPath, failedBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to GitError")
}

func getBranchSha(t *testing.T, cfg config.Cfg, repoPath string, branchName string) string {
	branchSha := string(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", branchName))
	return strings.TrimSpace(branchSha)
}

func TestRebaseRequestWithDeletedFile(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)
	repoProto, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	t.Cleanup(cleanup)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")
	defer cleanup()

	branch := "rebase-delete-test"

	gittest.Exec(t, cfg, "-C", repoPath, "config", "user.name", string(gittest.TestUser.Name))
	gittest.Exec(t, cfg, "-C", repoPath, "config", "user.email", string(gittest.TestUser.Email))
	gittest.Exec(t, cfg, "-C", repoPath, "checkout", "-b", branch, "master~1")
	gittest.Exec(t, cfg, "-C", repoPath, "rm", "README")
	gittest.Exec(t, cfg, "-C", repoPath, "commit", "-a", "-m", "delete file")

	branchSha := getBranchSha(t, cfg, repoPath, branch)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", branch, branchSha, repoCopyProto, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err, "look up git commit before rebase is applied")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	newBranchSha := getBranchSha(t, cfg, repoPath, branch)

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func TestRebaseOntoRemoteBranch(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	remoteRepo, remoteRepoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	defer cleanup()

	localBranch := "master"
	localBranchHash := getBranchSha(t, cfg, repoPath, localBranch)

	remoteBranch := "remote-branch"
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "config", "user.name", string(gittest.TestUser.Name))
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "config", "user.email", string(gittest.TestUser.Email))
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "checkout", "-b", remoteBranch, "master")
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "rm", "README")
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "commit", "-a", "-m", "remove README")
	remoteBranchHash := getBranchSha(t, cfg, remoteRepoPath, remoteBranch)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	_, err = repo.ReadCommit(ctx, git.Revision(remoteBranchHash))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "remote commit does not yet exist in local repository")

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", localBranch, localBranchHash, remoteRepo, remoteBranch)
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(remoteBranchHash))
	require.NoError(t, err, "remote commit does now exist in local repository")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	rebasedBranchHash := getBranchSha(t, cfg, repoPath, localBranch)

	require.NotEqual(t, rebasedBranchHash, localBranchHash)
	require.Equal(t, rebasedBranchHash, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func TestRebaseFailedWithCode(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	testCases := []struct {
		desc               string
		buildHeaderRequest func() *gitalypb.UserRebaseConfirmableRequest
		expectedCode       codes.Code
	}{
		{
			desc: "non-existing storage",
			buildHeaderRequest: func() *gitalypb.UserRebaseConfirmableRequest {
				repo := proto.Clone(repoProto).(*gitalypb.Repository)
				repo.StorageName = "@this-storage-does-not-exist"

				return buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, branchSha, repo, "master")
			},
			expectedCode: codes.InvalidArgument,
		},
		{
			desc: "missing repository path",
			buildHeaderRequest: func() *gitalypb.UserRebaseConfirmableRequest {
				repo := proto.Clone(repoProto).(*gitalypb.Repository)
				repo.RelativePath = ""

				return buildHeaderRequest(repo, gittest.TestUser, "1", rebaseBranchName, branchSha, repo, "master")
			},
			expectedCode: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rebaseStream, err := client.UserRebaseConfirmable(ctx)
			require.NoError(t, err)

			headerRequest := tc.buildHeaderRequest()
			require.NoError(t, rebaseStream.Send(headerRequest), "send header")

			_, err = rebaseStream.Recv()
			testhelper.RequireGrpcError(t, err, tc.expectedCode)
		})
	}
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
