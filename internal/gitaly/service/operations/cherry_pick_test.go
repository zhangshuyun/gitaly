package operations

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestServer_UserCherryPick_successful(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickSuccessful)
}

func testServerUserCherryPickSuccessful(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	masterHeadCommit, err := repo.ReadCommit(ctxOuter, "master")
	require.NoError(t, err)

	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	testRepoCopy, testRepoCopyPath, cleanup := testhelper.NewTestRepo(t) // read-only repo
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoCopyPath, "branch", destinationBranch, "master")

	testCases := []struct {
		desc         string
		request      *gitalypb.UserCherryPickRequest
		branchUpdate *gitalypb.OperationBranchUpdate
	}{
		{
			desc: "branch exists",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       testhelper.TestUser,
				Commit:     cherryPickedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      repoProto,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-1"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      repoProto,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-2"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartRepository: testRepoCopy,
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      repoProto,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-3"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "branch exists with dry run",
			request: &gitalypb.UserCherryPickRequest{
				Repository: testRepoCopy,
				User:       testhelper.TestUser,
				Commit:     cherryPickedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
				DryRun:     true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository with dry run",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      testRepoCopy,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-1"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository with dry run",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      testRepoCopy,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-2"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartRepository: testRepoCopy,
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository with dry run",
			request: &gitalypb.UserCherryPickRequest{
				Repository:      testRepoCopy,
				User:            testhelper.TestUser,
				Commit:          cherryPickedCommit,
				BranchName:      []byte("to-be-cherry-picked-into-3"),
				Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)

			mdFromCtx, ok := metadata.FromOutgoingContext(ctxOuter)
			if ok {
				md = metadata.Join(md, mdFromCtx)
			}

			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			response, err := client.UserCherryPick(ctx, testCase.request)
			require.NoError(t, err)

			testRepo := localrepo.New(git.NewExecCommandFactory(config.Config), testCase.request.Repository, config.Config)
			headCommit, err := testRepo.ReadCommit(ctx, git.Revision(testCase.request.BranchName))
			require.NoError(t, err)

			expectedBranchUpdate := testCase.branchUpdate
			expectedBranchUpdate.CommitId = headCommit.Id

			require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
			require.Empty(t, response.CreateTreeError)
			require.Empty(t, response.CreateTreeErrorCode)

			if testCase.request.DryRun {
				testhelper.ProtoEqual(t, masterHeadCommit, headCommit)
			} else {
				require.Equal(t, testCase.request.Message, headCommit.Subject)
				require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
			}
		})
	}
}

func TestServer_UserCherryPick_successful_git_hooks(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickSuccessfulGitHooks)
}

func testServerUserCherryPickSuccessfulGitHooks(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	var hookOutputFiles []string
	for _, hookName := range GitlabHooks {
		hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, repoPath, hookName)
		defer cleanup()
		hookOutputFiles = append(hookOutputFiles, hookOutputTempPath)
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)

	for _, file := range hookOutputFiles {
		output := string(testhelper.MustReadFile(t, file))
		require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
	}
}

func TestServer_UserCherryPick_stableID(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickStableID)
}

func testServerUserCherryPickStableID(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	commitToPick, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     commitToPick,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + commitToPick.Id),
		Timestamp:  &timestamp.Timestamp{Seconds: 12345},
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = metadata.NewOutgoingContext(ctx, md)

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)
	require.Equal(t, response.BranchUpdate.CommitId, "750e8cf248a67a0be1c5e3b891697d72c19af259")

	pickedCommit, err := repo.ReadCommit(ctx, "750e8cf248a67a0be1c5e3b891697d72c19af259")
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:        "750e8cf248a67a0be1c5e3b891697d72c19af259",
		Subject:   []byte("Cherry-picking " + commitToPick.Id),
		Body:      []byte("Cherry-picking " + commitToPick.Id),
		BodySize:  55,
		ParentIds: []string{"1e292f8fedd741b75372e19097c76d327140c312"},
		TreeId:    "5f1b6bcadf0abc482a19454aeaa219a5998db083",
		Author: &gitalypb.CommitAuthor{
			Name:  []byte("Ahmad Sherif"),
			Email: []byte("me@ahmadsherif.com"),
			Date: &timestamp.Timestamp{
				Seconds: 1487337076,
			},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:  testhelper.TestUser.Name,
			Email: testhelper.TestUser.Email,
			Date: &timestamp.Timestamp{
				Seconds: 12345,
			},
			Timezone: []byte("+0000"),
		},
	}, pickedCommit)
}

func TestServer_UserCherryPick_failed_validations(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickFailedValidations)
}

func testServerUserCherryPickFailedValidations(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	destinationBranch := "cherry-picking-dst"

	testCases := []struct {
		desc    string
		request *gitalypb.UserCherryPickRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       nil,
				Commit:     cherryPickedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       testhelper.TestUser,
				Commit:     nil,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       testhelper.TestUser,
				Commit:     cherryPickedCommit,
				BranchName: nil,
				Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty message",
			request: &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       testhelper.TestUser,
				Commit:     cherryPickedCommit,
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

			_, err := client.UserCherryPick(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestServer_UserCherryPick_failed_with_PreReceiveError(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickFailedWithPreReceiveError)
}

func testServerUserCherryPickFailedWithPreReceiveError(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove := testhelper.WriteCustomHook(t, repoPath, hookName, hookContent)
			defer remove()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			response, err := client.UserCherryPick(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
		})
	}
}

func TestServer_UserCherryPick_failed_with_CreateTreeError(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickFailedWithCreateTreeError)
}

func testServerUserCherryPickFailedWithCreateTreeError(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	// This commit already exists in master
	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, "4a24d82dbca5c11c61556f3b35ca472b7463187e")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserCherryPickResponse_EMPTY, response.CreateTreeErrorCode)
}

func TestServer_UserCherryPick_failed_with_CommitError(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickFailedWithCommitError)
}

func testServerUserCherryPickFailedWithCommitError(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	sourceBranch := "cherry-pick-src"
	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", sourceBranch, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")

	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, git.Revision(sourceBranch))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository:      repoProto,
		User:            testhelper.TestUser,
		Commit:          cherryPickedCommit,
		BranchName:      []byte(sourceBranch),
		Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
		StartBranchName: []byte(destinationBranch),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Branch diverged", response.CommitError)
}

func TestServer_UserCherryPick_failed_with_conflict(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickFailedWithConflict)
}

func testServerUserCherryPickFailedWithConflict(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "conflict_branch_a")

	// This commit cannot be applied to the destinationBranch above
	cherryPickedCommit, err := repo.ReadCommit(ctxOuter, git.Revision("f0f390655872bb2772c85a0128b2fbc2d88670cb"))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := testhelper.MergeOutgoingMetadata(ctxOuter, md)

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserCherryPickResponse_CONFLICT, response.CreateTreeErrorCode)
}

func TestServer_UserCherryPick_successful_with_given_commits(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCherryPick, testServerUserCherryPickSuccessfulWithGivenCommits)
}

func testServerUserCherryPickSuccessfulWithGivenCommits(t *testing.T, ctxOuter context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	testCases := []struct {
		desc           string
		startRevision  git.Revision
		cherryRevision git.Revision
	}{
		{
			desc:           "merge commit",
			startRevision:  "281d3a76f31c812dbf48abce82ccf6860adedd81",
			cherryRevision: "6907208d755b60ebeacb2e9dfea74c92c3449a1f",
		},
	}

	for i, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			md := testhelper.GitalyServersMetadata(t, serverSocketPath)

			mdFromCtx, ok := metadata.FromOutgoingContext(ctxOuter)
			if ok {
				md = metadata.Join(md, mdFromCtx)
			}

			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			destinationBranch := fmt.Sprintf("cherry-picking-%d", i)

			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, testCase.startRevision.String())

			commit, err := repo.ReadCommit(ctxOuter, testCase.cherryRevision)
			require.NoError(t, err)

			request := &gitalypb.UserCherryPickRequest{
				Repository: repoProto,
				User:       testhelper.TestUser,
				Commit:     commit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Cherry-picking " + testCase.cherryRevision.String()),
			}

			response, err := client.UserCherryPick(ctx, request)
			require.NoError(t, err)

			newHead, err := repo.ReadCommit(ctx, git.Revision(destinationBranch))
			require.NoError(t, err)

			expectedResponse := &gitalypb.UserCherryPickResponse{
				BranchUpdate: &gitalypb.OperationBranchUpdate{CommitId: newHead.Id},
			}

			testhelper.ProtoEqual(t, expectedResponse, response)

			require.Equal(t, request.Message, newHead.Subject)
			require.Equal(t, testCase.startRevision.String(), newHead.ParentIds[0])
		})
	}
}
