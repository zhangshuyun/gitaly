package operations

import (
	"context"
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

func testServerUserRevertSuccessful(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertSuccessfulFeatured)
}

func testServerUserRevertSuccessfulFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	masterHeadCommit, err := repo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	testRepoCopy, testRepoCopyPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "read-only") // read-only repo
	defer cleanup()

	gittest.Exec(t, cfg, "-C", testRepoCopyPath, "branch", destinationBranch, "master")

	testCases := []struct {
		desc         string
		request      *gitalypb.UserRevertRequest
		branchUpdate *gitalypb.OperationBranchUpdate
	}{
		{
			desc: "branch exists",
			request: &gitalypb.UserRevertRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository",
			request: &gitalypb.UserRevertRequest{
				Repository:      repoProto,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-1"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository",
			request: &gitalypb.UserRevertRequest{
				Repository:      repoProto,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-2"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartRepository: testRepoCopy,
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository",
			request: &gitalypb.UserRevertRequest{
				Repository:      repoProto,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-3"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "branch exists with dry run",
			request: &gitalypb.UserRevertRequest{
				Repository: testRepoCopy,
				User:       gittest.TestUser,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
				DryRun:     true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{},
		},
		{
			desc: "nonexistent branch + start_repository == repository with dry run",
			request: &gitalypb.UserRevertRequest{
				Repository:      testRepoCopy,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-1"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + start_repository != repository with dry run",
			request: &gitalypb.UserRevertRequest{
				Repository:      testRepoCopy,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-2"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartRepository: testRepoCopy,
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
		{
			desc: "nonexistent branch + empty start_repository with dry run",
			request: &gitalypb.UserRevertRequest{
				Repository:      testRepoCopy,
				User:            gittest.TestUser,
				Commit:          revertedCommit,
				BranchName:      []byte("to-be-reverted-into-3"),
				Message:         []byte("Reverting " + revertedCommit.Id),
				StartBranchName: []byte("master"),
				DryRun:          true,
			},
			branchUpdate: &gitalypb.OperationBranchUpdate{BranchCreated: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			response, err := client.UserRevert(ctx, testCase.request)
			require.NoError(t, err)

			testCaseRepo := localrepo.NewTestRepo(t, cfg, testCase.request.Repository)
			headCommit, err := testCaseRepo.ReadCommit(ctx, git.Revision(testCase.request.BranchName))
			require.NoError(t, err)

			expectedBranchUpdate := testCase.branchUpdate
			expectedBranchUpdate.CommitId = headCommit.Id

			require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
			require.Empty(t, response.CreateTreeError)
			require.Empty(t, response.CreateTreeErrorCode)

			if testCase.request.DryRun {
				require.Equal(t, masterHeadCommit.Subject, headCommit.Subject)
				require.Equal(t, masterHeadCommit.Id, headCommit.Id)
			} else {
				require.Equal(t, testCase.request.Message, headCommit.Subject)
				require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
			}
		})
	}
}

func testServerUserRevertStableID(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertStableIDFeatured)
}

func testServerUserRevertStableIDFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	commitToRevert, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	response, err := client.UserRevert(ctx, &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     commitToRevert,
		BranchName: []byte("master"),
		Message:    []byte("Reverting commit"),
		Timestamp:  &timestamp.Timestamp{Seconds: 12345},
	})
	require.NoError(t, err)

	require.Equal(t, &gitalypb.OperationBranchUpdate{
		CommitId: "9ebfd44039a9e36d88dcdfe11550399ec6a212f7",
	}, response.BranchUpdate)
	require.Empty(t, response.CreateTreeError)
	require.Empty(t, response.CreateTreeErrorCode)

	revertedCommit, err := repo.ReadCommit(ctx, git.Revision("master"))
	require.NoError(t, err)

	require.Equal(t, &gitalypb.GitCommit{
		Id: "9ebfd44039a9e36d88dcdfe11550399ec6a212f7",
		ParentIds: []string{
			"1e292f8fedd741b75372e19097c76d327140c312",
		},
		TreeId:   "3a1de94946517a42fcfe4bf4986b8c61af799bd5",
		Subject:  []byte("Reverting commit"),
		Body:     []byte("Reverting commit"),
		BodySize: 16,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Jane Doe"),
			Email:    []byte("janedoe@gitlab.com"),
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     []byte("Jane Doe"),
			Email:    []byte("janedoe@gitlab.com"),
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, revertedCommit)
}

func testServerUserRevertSuccessfulIntoEmptyRepo(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertSuccessfulIntoNewRepo)
}

func testServerUserRevertSuccessfulIntoNewRepo(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, startRepoProto, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	startRepo := localrepo.NewTestRepo(t, cfg, startRepoProto)

	revertedCommit, err := startRepo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	masterHeadCommit, err := startRepo.ReadCommit(ctx, "master")
	require.NoError(t, err)

	repoProto, _, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	request := &gitalypb.UserRevertRequest{
		Repository:      repoProto,
		User:            gittest.TestUser,
		Commit:          revertedCommit,
		BranchName:      []byte("dst-branch"),
		Message:         []byte("Reverting " + revertedCommit.Id),
		StartRepository: startRepoProto,
		StartBranchName: []byte("master"),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)

	headCommit, err := repo.ReadCommit(ctx, git.Revision(request.BranchName))
	require.NoError(t, err)

	expectedBranchUpdate := &gitalypb.OperationBranchUpdate{
		BranchCreated: true,
		RepoCreated:   true,
		CommitId:      headCommit.Id,
	}

	require.Equal(t, expectedBranchUpdate, response.BranchUpdate)
	require.Empty(t, response.CreateTreeError)
	require.Empty(t, response.CreateTreeErrorCode)
	require.Equal(t, request.Message, headCommit.Subject)
	require.Equal(t, masterHeadCommit.Id, headCommit.ParentIds[0])
}

func testServerUserRevertSuccessfulGitHooks(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertSuccessfulGitHooksFeatured)
}

func testServerUserRevertSuccessfulGitHooksFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	var hookOutputFiles []string
	for _, hookName := range GitlabHooks {
		hookOutputTempPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)
		hookOutputFiles = append(hookOutputFiles, hookOutputTempPath)
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.PreReceiveError)

	for _, file := range hookOutputFiles {
		output := string(testhelper.MustReadFile(t, file))
		require.Contains(t, output, "GL_USERNAME="+gittest.TestUser.GlUsername)
	}
}

func testServerUserRevertFailuedDueToValidations(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertFailuedDueToValidationsFeatured)
}

func testServerUserRevertFailuedDueToValidationsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, _, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	destinationBranch := "revert-dst"

	testCases := []struct {
		desc    string
		request *gitalypb.UserRevertRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserRevertRequest{
				Repository: repoProto,
				User:       nil,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit",
			request: &gitalypb.UserRevertRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     nil,
				BranchName: []byte(destinationBranch),
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserRevertRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     revertedCommit,
				BranchName: nil,
				Message:    []byte("Reverting " + revertedCommit.Id),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty message",
			request: &gitalypb.UserRevertRequest{
				Repository: repoProto,
				User:       gittest.TestUser,
				Commit:     revertedCommit,
				BranchName: []byte(destinationBranch),
				Message:    nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			_, err := client.UserRevert(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func testServerUserRevertFailedDueToPreReceiveError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertFailedDueToPreReceiveErrorFeatured)
}

func testServerUserRevertFailedDueToPreReceiveErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			gittest.WriteCustomHook(t, repoPath, hookName, hookContent)

			response, err := client.UserRevert(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+gittest.TestUser.GlId)
		})
	}
}

func testServerUserRevertFailedDueToCreateTreeErrorConflict(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertFailedDueToCreateTreeErrorConflictFeatured)
}

func testServerUserRevertFailedDueToCreateTreeErrorConflictFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	// This revert patch of the following commit cannot be applied to the destinationBranch above
	revertedCommit, err := repo.ReadCommit(ctx, "372ab6950519549b14d220271ee2322caa44d4eb")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_CONFLICT, response.CreateTreeErrorCode)
}

func testServerUserRevertFailedDueToCreateTreeErrorEmpty(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertFailedDueToCreateTreeErrorEmptyFeatured)
}

func testServerUserRevertFailedDueToCreateTreeErrorEmptyFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")

	revertedCommit, err := repo.ReadCommit(ctx, "d59c60028b053793cecfb4022de34602e1a9218e")
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository: repoProto,
		User:       gittest.TestUser,
		Commit:     revertedCommit,
		BranchName: []byte(destinationBranch),
		Message:    []byte("Reverting " + revertedCommit.Id),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_NONE, response.CreateTreeErrorCode)

	response, err = client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, response.CreateTreeError)
	require.Equal(t, gitalypb.UserRevertResponse_EMPTY, response.CreateTreeErrorCode)
}

func testServerUserRevertFailedDueToCommitError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testWithFeature(t, featureflag.GoUserRevert, cfg, rubySrv, testServerUserRevertFailedDueToCommitErrorFeatured)
}

func testServerUserRevertFailedDueToCommitErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cfg, repoProto, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, rubySrv)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	sourceBranch := "revert-src"
	destinationBranch := "revert-dst"
	gittest.Exec(t, cfg, "-C", repoPath, "branch", destinationBranch, "master")
	gittest.Exec(t, cfg, "-C", repoPath, "branch", sourceBranch, "a5391128b0ef5d21df5dd23d98557f4ef12fae20")

	revertedCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)

	request := &gitalypb.UserRevertRequest{
		Repository:      repoProto,
		User:            gittest.TestUser,
		Commit:          revertedCommit,
		BranchName:      []byte(destinationBranch),
		Message:         []byte("Reverting " + revertedCommit.Id),
		StartBranchName: []byte(sourceBranch),
	}

	response, err := client.UserRevert(ctx, request)
	require.NoError(t, err)
	require.Equal(t, "Branch diverged", response.CommitError)
}
