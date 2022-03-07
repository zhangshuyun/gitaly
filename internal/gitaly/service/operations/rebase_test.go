package operations

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var rebaseBranchName = "many_files"

func TestSuccessfulUserRebaseConfirmableRequest(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	pushOptions := []string{"ci.skip", "test=value"}
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg, pushOptions...)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

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
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err)

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

func TestUserRebaseConfirmable_skipEmptyCommits(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	// This is the base commit from which both "theirs" and "ours" branch from".
	baseCommit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "a\nb\nc\nd\ne\nf\n"},
		),
	)

	// "theirs" changes the first line of the file to contain a "1".
	theirs := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(baseCommit),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\nf\n"},
		),
		gittest.WithMessage("theirs"),
		gittest.WithBranch("theirs"),
	)

	// We create two commits on "ours": the first one does the same changes as "theirs", but
	// with a different commit ID. It is expected to become empty. And the second commit is an
	// independent change which modifies the last line.
	oursBecomingEmpty := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(baseCommit),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\nf\n"},
		),
		gittest.WithMessage("ours but same change as theirs"),
	)
	ours := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithParents(oursBecomingEmpty),
		gittest.WithTreeEntries(
			gittest.TreeEntry{Mode: "100644", Path: "README", Content: "1\nb\nc\nd\ne\n6\n"},
		),
		gittest.WithMessage("ours with additional changes"),
		gittest.WithBranch("ours"),
	)

	stream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.UserRebaseConfirmableRequest{
		UserRebaseConfirmableRequestPayload: &gitalypb.UserRebaseConfirmableRequest_Header_{
			Header: &gitalypb.UserRebaseConfirmableRequest_Header{
				Repository: repoProto,
				User:       gittest.TestUser,
				RebaseId:   "something",
				// Note: the way UserRebaseConfirmable handles BranchSha and
				// RemoteBranch is really weird. What we're doing is to rebase
				// BranchSha on top of RemoteBranch, even though documentation of
				// the protobuf says that we're doing it the other way round. I
				// don't dare changing this now though, so I simply abide and write
				// the test with those weird semantics.
				Branch:           []byte("ours"),
				BranchSha:        ours.String(),
				RemoteRepository: repoProto,
				RemoteBranch:     []byte("theirs"),
				Timestamp:        &timestamppb.Timestamp{Seconds: 123456},
			},
		},
	}))

	response, err := stream.Recv()
	require.NoError(t, err)
	require.NoError(t, stream.Send(buildApplyRequest(true)))

	rebaseOID := git.ObjectID(response.GetRebaseSha())

	response, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, response.GetRebaseApplied())

	rebaseCommit, err := localrepo.NewTestRepo(t, cfg, repoProto).ReadCommit(ctx, rebaseOID.Revision())
	require.NoError(t, err)
	testhelper.ProtoEqual(t, &gitalypb.GitCommit{
		Subject:   []byte("ours with additional changes"),
		Body:      []byte("ours with additional changes"),
		BodySize:  28,
		Id:        "ef7f98be1f753f1a9fa895d999a855611d691629",
		ParentIds: []string{theirs.String()},
		TreeId:    "b68aeb18813d7f2e180f2cc0bccc128511438b29",
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Scrooge McDuck"),
			Email:    []byte("scrooge@mcduck.com"),
			Date:     &timestamppb.Timestamp{Seconds: 1572776879},
			Timezone: []byte("+0100"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     gittest.TestUser.Name,
			Email:    gittest.TestUser.Email,
			Date:     &timestamppb.Timestamp{Seconds: 123456},
			Timezone: []byte("+0000"),
		},
	}, rebaseCommit)
}

func TestUserRebaseConfirmableTransaction(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	txManager := transaction.NewTrackingManager()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(
		t, ctx,
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

			txManager.Reset()

			ctx := ctx
			if tc.withTransaction {
				ctx = metadata.OutgoingToIncoming(ctx)

				var err error
				ctx, err = txinfo.InjectTransaction(ctx, 1, "node", tc.primary)
				require.NoError(t, err)
				ctx = metadata.IncomingToOutgoing(ctx)
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

			require.Equal(t, tc.expectedVotes, len(txManager.Votes()))
			if tc.expectPreReceiveHook {
				require.FileExists(t, preReceiveHookOutputPath)
			} else {
				require.NoFileExists(t, preReceiveHookOutputPath)
			}
		})
	}
}

func TestUserRebaseConfirmableStableCommitIDs(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	cfg.Gitlab.URL = setupAndStartGitlabServer(t, gittest.GlID, "project-1", cfg)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	committerDate := &timestamppb.Timestamp{Seconds: 100000000}
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
			Date:     &timestamppb.Timestamp{Seconds: 1510610637},
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
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repo, repoPath, client := setupOperationsService(t, ctx)

	repoCopy, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])

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
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
			require.Empty(t, firstResponse.GetRebaseSha(), "rebase sha on first response")
		})
	}
}

func TestAbortedUserRebaseConfirmable(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

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
			createRepoOpts := gittest.CreateRepositoryConfig{Seed: gittest.SeedGitLabTest}
			testRepo, testRepoPath := gittest.CreateRepository(ctx, t, cfg, createRepoOpts)
			testRepoCopy, _ := gittest.CreateRepository(ctx, t, cfg, createRepoOpts)

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
			testhelper.RequireGrpcCode(t, err, tc.code)

			newBranchSha := getBranchSha(t, cfg, testRepoPath, rebaseBranchName)
			require.Equal(t, newBranchSha, branchSha, "branch should not change when the rebase is aborted")
		})
	}
}

func TestFailedUserRebaseConfirmableDueToApplyBeingFalse(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testRepoCopy, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	branchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)

	rebaseStream, err := client.UserRebaseConfirmable(ctx)
	require.NoError(t, err)

	headerRequest := buildHeaderRequest(repoProto, gittest.TestUser, "1", rebaseBranchName, branchSha, testRepoCopy, "master")
	require.NoError(t, rebaseStream.Send(headerRequest), "send header")

	firstResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive first response")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildApplyRequest(false)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.Error(t, err, "second response should have error")
	testhelper.RequireGrpcCode(t, err, codes.FailedPrecondition)
	require.False(t, secondResponse.GetRebaseApplied(), "the second rebase is not applied")

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should have been discarded")

	newBranchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)
	require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase is not applied")
	require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase is not applied")
}

func TestFailedUserRebaseConfirmableRequestDueToPreReceiveError(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

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
			require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

			applyRequest := buildApplyRequest(true)
			require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

			secondResponse, err := rebaseStream.Recv()

			require.NoError(t, err, "receive second response")
			require.Contains(t, secondResponse.PreReceiveError, "failure")

			_, err = rebaseStream.Recv()
			require.Equal(t, io.EOF, err)

			_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
			if hookName == "pre-receive" {
				require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should have been discarded")
			} else {
				require.NoError(t, err)
			}

			newBranchSha := getBranchSha(t, cfg, repoPath, rebaseBranchName)
			require.Equal(t, branchSha, newBranchSha, "branch should not change when the rebase fails due to PreReceiveError")
			require.NotEqual(t, newBranchSha, firstResponse.GetRebaseSha(), "branch should not be the sha returned when the rebase fails due to PreReceiveError")
		})
	}
}

func TestUserRebaseConfirmable_gitError(t *testing.T) {
	testhelper.NewFeatureSets(featureflag.UserRebaseConfirmableImprovedErrorHandling).
		Run(t, testFailedUserRebaseConfirmableDueToGitError)
}

func testFailedUserRebaseConfirmableDueToGitError(t *testing.T, ctx context.Context) {
	t.Parallel()

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repoCopyProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

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
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)
	gittest.AddWorktree(t, cfg, repoPath, "worktree")
	repoPath = filepath.Join(repoPath, "worktree")

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	repoCopyProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

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
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	newBranchSha := getBranchSha(t, cfg, repoPath, branch)

	_, err = repo.ReadCommit(ctx, git.Revision(firstResponse.GetRebaseSha()))
	require.NoError(t, err, "look up git commit after rebase is applied")

	require.NotEqual(t, newBranchSha, branchSha)
	require.Equal(t, newBranchSha, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func TestRebaseOntoRemoteBranch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	remoteRepo, remoteRepoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})
	gittest.AddWorktree(t, cfg, remoteRepoPath, "worktree")
	remoteRepoPath = filepath.Join(remoteRepoPath, "worktree")

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
	require.Equal(t, localrepo.ErrObjectNotFound, err, "commit should not exist in the normal repo given that it is quarantined")

	applyRequest := buildApplyRequest(true)
	require.NoError(t, rebaseStream.Send(applyRequest), "apply rebase")

	secondResponse, err := rebaseStream.Recv()
	require.NoError(t, err, "receive second response")

	_, err = rebaseStream.Recv()
	require.Equal(t, io.EOF, err)

	_, err = repo.ReadCommit(ctx, git.Revision(remoteBranchHash))
	require.NoError(t, err)

	rebasedBranchHash := getBranchSha(t, cfg, repoPath, localBranch)

	require.NotEqual(t, rebasedBranchHash, localBranchHash)
	require.Equal(t, rebasedBranchHash, firstResponse.GetRebaseSha())

	require.True(t, secondResponse.GetRebaseApplied(), "the second rebase is applied")
}

func TestRebaseFailedWithCode(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

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
			testhelper.RequireGrpcCode(t, err, tc.expectedCode)
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
