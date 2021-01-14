package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(config.Config)
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	branchNameInput := "master"
	branchTarget, err := log.GetCommit(ctx, locator, testRepo, git.Revision(branchNameInput))
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

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.FindBranch(ctx, request)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch, "mismatched branches")
		})
	}
}

func TestFailedFindBranchRequest(t *testing.T) {
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

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

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.FindBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
