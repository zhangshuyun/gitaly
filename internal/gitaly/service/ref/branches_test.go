package ref

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindBranchRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepoProto, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repo := localrepo.New(testRepoProto, config.Config)

	branchesByName := make(map[git.ReferenceName]*gitalypb.Branch)
	for branchName, revision := range map[git.ReferenceName]git.Revision{
		"refs/heads/branch":            "refs/heads/master~0",
		"refs/heads/heads/branch":      "refs/heads/master~1",
		"refs/heads/refs/heads/branch": "refs/heads/master~2",
	} {
		oid, err := repo.ResolveRevision(ctx, revision)
		require.NoError(t, err)

		err = repo.UpdateRef(ctx, branchName, oid, "")
		require.NoError(t, err)

		commit, err := log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepoProto, branchName.Revision())
		require.NoError(t, err)

		branchesByName[branchName] = &gitalypb.Branch{
			Name:         []byte(branchName.String()[len("refs/heads/"):]),
			TargetCommit: commit,
		}
	}

	testCases := []struct {
		desc           string
		branchName     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:           "regular branch name",
			branchName:     "branch",
			expectedBranch: branchesByName["refs/heads/branch"],
		},
		{
			desc:           "absolute reference path",
			branchName:     "heads/branch",
			expectedBranch: branchesByName["refs/heads/heads/branch"],
		},
		{
			desc:           "heads path",
			branchName:     "refs/heads/branch",
			expectedBranch: branchesByName["refs/heads/refs/heads/branch"],
		},
		{
			desc:       "non-existent branch",
			branchName: "i-do-not-exist-on-this-repo",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.FindBranchRequest{
				Repository: testRepoProto,
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
