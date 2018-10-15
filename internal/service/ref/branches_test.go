package ref

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	headCommit, err := log.GetCommit(ctx, testRepo, "HEAD")
	require.NoError(t, err)

	startPoint := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	startPointCommit, err := log.GetCommit(ctx, testRepo, startPoint)
	require.NoError(t, err)

	testCases := []struct {
		desc           string
		startPoint     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:       "empty start point",
			startPoint: "",
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("to-be-created-soon-1"),
				TargetCommit: headCommit,
			},
		},
		{
			desc:       "present start point",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("to-be-created-soon-2"),
				TargetCommit: startPointCommit,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			branchName := testCase.expectedBranch.Name
			request := &gitalypb.CreateBranchRequest{
				Repository: testRepo,
				Name:       branchName,
				StartPoint: []byte(testCase.startPoint),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			response, err := client.CreateBranch(ctx, request)
			defer exec.Command("git", "-C", testRepoPath, "branch", "-D", string(branchName)).Run()

			require.NoError(t, err)
			require.Equal(t, gitalypb.CreateBranchResponse_OK, response.Status, "mismatched status")
			require.Equal(t, testCase.expectedBranch, response.Branch, "mismatched branches")
		})
	}
}

func TestFailedCreateBranchRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		branchName string
		startPoint string
		status     gitalypb.CreateBranchResponse_Status
	}{
		{
			desc:       "branch exists",
			branchName: "master",
			status:     gitalypb.CreateBranchResponse_ERR_EXISTS,
		},
		{
			desc:       "empty branch name",
			branchName: "",
			status:     gitalypb.CreateBranchResponse_ERR_INVALID,
		},
		{
			desc:       "invalid start point",
			branchName: "shiny-new-branch",
			startPoint: "i-do-not-exist",
			status:     gitalypb.CreateBranchResponse_ERR_INVALID_START_POINT,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.CreateBranchRequest{
				Repository: testRepo,
				Name:       []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			response, err := client.CreateBranch(ctx, request)

			require.NoError(t, err)
			require.Equal(t, testCase.status, response.Status, "mismatched status")
		})
	}
}

func TestSuccessfulDeleteBranchRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	branchNameInput := "to-be-deleted-soon"

	defer exec.Command("git", "-C", testRepoPath, "branch", "-D", branchNameInput).Run()

	testCases := []struct {
		desc       string
		branchName string
	}{
		{
			desc:       "regular branch name",
			branchName: branchNameInput,
		},
		{
			desc:       "absolute reference path",
			branchName: "refs/heads/" + branchNameInput,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)

			request := &gitalypb.DeleteBranchRequest{
				Repository: testRepo,
				Name:       []byte(testCase.branchName),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.DeleteBranch(ctx, request)
			require.NoError(t, err)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.NotContains(t, branches, branchNameInput, "branch name exists in branches list")
		})
	}
}

func TestFailedDeleteBranchRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		branchName string
		code       codes.Code
	}{
		{
			desc:       "branch does not exist",
			branchName: "this-branch-does-not-exist",
			code:       codes.Internal,
		},
		{
			desc:       "empty branch name",
			branchName: "",
			code:       codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.DeleteBranchRequest{
				Repository: testRepo,
				Name:       []byte(testCase.branchName),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.DeleteBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestSuccessfulFindBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	branchNameInput := "master"
	branchTarget, err := log.GetCommit(ctx, testRepo, branchNameInput)
	require.NoError(t, err)

	branch := &gitalypb.Branch{
		Name:         []byte(branchNameInput),
		TargetCommit: branchTarget,
	}

	testCases := []struct {
		desc           string
		branchName     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:           "regular branch name",
			branchName:     branchNameInput,
			expectedBranch: branch,
		},
		{
			desc:           "absolute reference path",
			branchName:     "refs/heads/" + branchNameInput,
			expectedBranch: branch,
		},
		{
			desc:           "heads path",
			branchName:     "heads/" + branchNameInput,
			expectedBranch: branch,
		},
		{
			desc:       "non-existent branch",
			branchName: "i-do-not-exist-on-this-repo",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: testRepo,
				Name:       []byte(testCase.branchName),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			response, err := client.FindBranch(ctx, request)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch, "mismatched branches")
		})
	}
}

func TestFailedFindBranchRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		branchName string
		code       codes.Code
	}{
		{
			desc:       "empty branch name",
			branchName: "",
			code:       codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {

			request := &gitalypb.FindBranchRequest{
				Repository: testRepo,
				Name:       []byte(testCase.branchName),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.FindBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
