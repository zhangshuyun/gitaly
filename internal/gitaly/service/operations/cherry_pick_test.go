package operations

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func testServerUserCherryPickSuccessful(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickSuccessfulFeatured)
}

func testServerUserCherryPickSuccessfulFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	masterHeadCommit, err := repo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
	require.NoError(t, err)

	testRepoCopy, testRepoCopyPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "read-only") // read-only repo
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
			response, err := client.UserCherryPick(ctx, testCase.request)
			require.NoError(t, err)

			testRepo := localrepo.New(git.NewExecCommandFactory(cfg), testCase.request.Repository, cfg)
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

func testServerUserCherryPickSuccessfulGitHooks(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickSuccessfulGitHooksFeatured)
}

func testServerUserCherryPickSuccessfulGitHooksFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
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
		hookOutputTempPath, cleanup := gittest.WriteEnvToCustomHook(t, repoPath, hookName)
		defer cleanup()
		hookOutputFiles = append(hookOutputFiles, hookOutputTempPath)
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)

	for _, file := range hookOutputFiles {
		output := string(testhelper.MustReadFile(t, file))
		require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
	}
}

func testServerUserCherryPickStableID(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickStableIDFeatured)
}

func testServerUserCherryPickStableIDFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

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

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)
	require.Equal(t, "b17aeac93194cf2385b32623494ebce66efbacad", response.BranchUpdate.CommitId)

	pickedCommit, err := repo.ReadCommit(ctx, git.Revision(response.BranchUpdate.CommitId))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:        "b17aeac93194cf2385b32623494ebce66efbacad",
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
			Timezone: []byte("+0200"),
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

func testServerUserCherryPickFailedValidations(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickFailedValidationsFeatured)
}

func testServerUserCherryPickFailedValidationsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
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
			_, err := client.UserCherryPick(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func testServerUserCherryPickFailedWithPreReceiveError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickFailedWithPreReceiveErrorFeatured)
}

func testServerUserCherryPickFailedWithPreReceiveErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	cherryPickedCommit, err := repo.ReadCommit(ctx, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")
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
			remove := gittest.WriteCustomHook(t, repoPath, hookName, hookContent)
			defer remove()

			response, err := client.UserCherryPick(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
		})
	}
}

func testServerUserCherryPickFailedWithCreateTreeError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickFailedWithCreateTreeErrorFeatured)
}

func testServerUserCherryPickFailedWithCreateTreeErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")

	// This commit already exists in master
	cherryPickedCommit, err := repo.ReadCommit(ctx, "4a24d82dbca5c11c61556f3b35ca472b7463187e")
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserCherryPickResponse_EMPTY, response.CreateTreeErrorCode)
}

func testServerUserCherryPickFailedWithCommitError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickFailedWithCommitErrorFeatured)
}

func testServerUserCherryPickFailedWithCommitErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	sourceBranch := "cherry-pick-src"
	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "master")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", sourceBranch, "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab")

	cherryPickedCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository:      repoProto,
		User:            testhelper.TestUser,
		Commit:          cherryPickedCommit,
		BranchName:      []byte(sourceBranch),
		Message:         []byte("Cherry-picking " + cherryPickedCommit.Id),
		StartBranchName: []byte(destinationBranch),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Branch diverged", response.CommitError)
}

func testServerUserCherryPickFailedWithConflict(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickFailedWithConflictFeatured)
}

func testServerUserCherryPickFailedWithConflictFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	destinationBranch := "cherry-picking-dst"
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, "conflict_branch_a")

	// This commit cannot be applied to the destinationBranch above
	cherryPickedCommit, err := repo.ReadCommit(ctx, git.Revision("f0f390655872bb2772c85a0128b2fbc2d88670cb"))
	require.NoError(t, err)

	request := &gitalypb.UserCherryPickRequest{
		Repository: repoProto,
		User:       testhelper.TestUser,
		Commit:     cherryPickedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Cherry-picking " + cherryPickedCommit.Id),
	}

	response, err := client.UserCherryPick(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserCherryPickResponse_CONFLICT, response.CreateTreeErrorCode)
}

func testServerUserCherryPickSuccessfulWithGivenCommits(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserCherryPick, cfg, rubySrv, testServerUserCherryPickSuccessfulWithGivenCommitsFeatured)
}

func testServerUserCherryPickSuccessfulWithGivenCommitsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

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
			destinationBranch := fmt.Sprintf("cherry-picking-%d", i)

			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", destinationBranch, testCase.startRevision.String())

			commit, err := repo.ReadCommit(ctx, testCase.cherryRevision)
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
