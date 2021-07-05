package commit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindAllCommitsRequest(t *testing.T) {
	defer func() {
		_findBranchNamesFunc = ref.FindBranchNames
	}()

	_findBranchNamesFunc = func(context.Context, git.RepositoryExecutor) ([][]byte, error) {
		return [][]byte{
			[]byte("few-commits"),
			[]byte("two-commits"),
		}, nil
	}

	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	// Commits made on another branch in parallel to the normal commits below.
	// Will be used to test topology ordering.
	alternateCommits := []*gitalypb.GitCommit{
		gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
		gittest.CommitsByID["48ca272b947f49eee601639d743784a176574a09"],
		gittest.CommitsByID["335bc94d5b7369b10251e612158da2e4a4aaa2a5"],
	}

	// Nothing special about these commits.
	normalCommits := []*gitalypb.GitCommit{
		gittest.CommitsByID["bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb"],
		gittest.CommitsByID["9d526f87b82e2b2fd231ca44c95508e5e85624ca"],
		gittest.CommitsByID["1039376155a0d507eba0ea95c29f8f5b983ea34b"],
		gittest.CommitsByID["54188278422b1fa877c2e71c4e37fc6640a58ad1"],
		gittest.CommitsByID["8b9270332688d58e25206601900ee5618fab2390"],
		gittest.CommitsByID["f9220df47bce1530e90c189064d301bfc8ceb5ab"],
		gittest.CommitsByID["40d408f89c1fd26b7d02e891568f880afe06a9f8"],
		gittest.CommitsByID["df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"],
		gittest.CommitsByID["6762605237fc246ae146ac64ecb467f71d609120"],
		gittest.CommitsByID["79b06233d3dc769921576771a4e8bee4b439595d"],
		gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
	}

	// A commit that exists on "two-commits" branch.
	singleCommit := []*gitalypb.GitCommit{
		gittest.CommitsByID["304d257dcb821665ab5110318fc58a007bd104ed"],
	}

	timeOrderedCommits := []*gitalypb.GitCommit{
		alternateCommits[0], normalCommits[0],
		alternateCommits[1], normalCommits[1],
		alternateCommits[2],
	}
	timeOrderedCommits = append(timeOrderedCommits, normalCommits[2:]...)
	topoOrderedCommits := append(alternateCommits, normalCommits...)

	testCases := []struct {
		desc            string
		request         *gitalypb.FindAllCommitsRequest
		expectedCommits []*gitalypb.GitCommit
	}{
		{
			desc: "all commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits,
		},
		{
			desc: "maximum number of commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				MaxCount: 5,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[:5],
		},
		{
			desc: "skipping number of commits of a revision",
			request: &gitalypb.FindAllCommitsRequest{
				Skip:     5,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[5:],
		},
		{
			desc: "maximum number of commits of a revision plus skipping",
			request: &gitalypb.FindAllCommitsRequest{
				Skip:     5,
				MaxCount: 2,
				Revision: []byte("few-commits"),
			},
			expectedCommits: timeOrderedCommits[5:7],
		},
		{
			desc: "all commits of a revision ordered by date",
			request: &gitalypb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
				Order:    gitalypb.FindAllCommitsRequest_DATE,
			},
			expectedCommits: timeOrderedCommits,
		},
		{
			desc: "all commits of a revision ordered by topology",
			request: &gitalypb.FindAllCommitsRequest{
				Revision: []byte("few-commits"),
				Order:    gitalypb.FindAllCommitsRequest_TOPO,
			},
			expectedCommits: topoOrderedCommits,
		},
		{
			desc:            "all commits of all branches",
			request:         &gitalypb.FindAllCommitsRequest{},
			expectedCommits: append(singleCommit, timeOrderedCommits...),
		},
		{
			desc:            "non-existing revision",
			request:         &gitalypb.FindAllCommitsRequest{Revision: []byte("i-do-not-exist")},
			expectedCommits: []*gitalypb.GitCommit{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := testCase.request
			request.Repository = repo

			ctx, cancel := testhelper.Context()
			defer cancel()
			c, err := client.FindAllCommits(ctx, request)
			require.NoError(t, err)

			receivedCommits := getAllCommits(t, func() (gitCommitsGetter, error) { return c.Recv() })

			require.Equal(t, len(testCase.expectedCommits), len(receivedCommits), "number of commits received")

			for i, receivedCommit := range receivedCommits {
				testassert.ProtoEqual(t, testCase.expectedCommits[i], receivedCommit)
			}
		})
	}
}

func TestFailedFindAllCommitsRequest(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.FindAllCommitsRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.FindAllCommitsRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.FindAllCommitsRequest{},
			code:    codes.InvalidArgument,
		},
		{
			desc: "Revision is invalid",
			request: &gitalypb.FindAllCommitsRequest{
				Repository: repo,
				Revision:   []byte("--output=/meow"),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			c, err := client.FindAllCommits(ctx, testCase.request)
			require.NoError(t, err)

			err = drainFindAllCommitsResponse(c)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func drainFindAllCommitsResponse(c gitalypb.CommitService_FindAllCommitsClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
