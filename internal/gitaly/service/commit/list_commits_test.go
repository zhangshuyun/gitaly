package commit

import (
	"errors"
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestListCommits(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	for _, tc := range []struct {
		desc            string
		request         *gitalypb.ListCommitsRequest
		expectedCommits []*gitalypb.GitCommit
		expectedErr     error
	}{
		{
			desc: "single revision",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
				gittest.CommitsByID["bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb"],
				gittest.CommitsByID["48ca272b947f49eee601639d743784a176574a09"],
				gittest.CommitsByID["9d526f87b82e2b2fd231ca44c95508e5e85624ca"],
				gittest.CommitsByID["335bc94d5b7369b10251e612158da2e4a4aaa2a5"],
				gittest.CommitsByID["1039376155a0d507eba0ea95c29f8f5b983ea34b"],
				gittest.CommitsByID["54188278422b1fa877c2e71c4e37fc6640a58ad1"],
				gittest.CommitsByID["8b9270332688d58e25206601900ee5618fab2390"],
				gittest.CommitsByID["f9220df47bce1530e90c189064d301bfc8ceb5ab"],
				gittest.CommitsByID["40d408f89c1fd26b7d02e891568f880afe06a9f8"],
				gittest.CommitsByID["df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"],
				gittest.CommitsByID["6762605237fc246ae146ac64ecb467f71d609120"],
				gittest.CommitsByID["79b06233d3dc769921576771a4e8bee4b439595d"],
				gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
			},
		},
		{
			desc: "single revision with limit",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				PaginationParams: &gitalypb.PaginationParameter{
					Limit: 2,
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
				gittest.CommitsByID["bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb"],
			},
		},
		{
			desc: "single revision with page token",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				PaginationParams: &gitalypb.PaginationParameter{
					PageToken: "79b06233d3dc769921576771a4e8bee4b439595d",
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
			},
		},
		{
			desc: "revision range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"^0031876facac3f2b2702a0e53a26e89939a42209~",
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
				gittest.CommitsByID["48ca272b947f49eee601639d743784a176574a09"],
				gittest.CommitsByID["335bc94d5b7369b10251e612158da2e4a4aaa2a5"],
			},
		},
		{
			desc: "reverse revision range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"^0031876facac3f2b2702a0e53a26e89939a42209~",
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				Reverse: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["335bc94d5b7369b10251e612158da2e4a4aaa2a5"],
				gittest.CommitsByID["48ca272b947f49eee601639d743784a176574a09"],
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
			},
		},
		{
			desc: "revisions with sort topo order",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"master~2",
					"^master~4",
					"flat-path",
					"^flat-path~",
				},
				Order: gitalypb.ListCommitsRequest_TOPO,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["60ecb67744cb56576c30214ff52294f8ce2def98"],
				gittest.CommitsByID["55bc176024cfa3baaceb71db584c7e5df900ea65"],
				gittest.CommitsByID["e63f41fe459e62e1228fcef60d7189127aeba95a"],
				gittest.CommitsByID["4a24d82dbca5c11c61556f3b35ca472b7463187e"],
				// This commit is sorted differently compared to the following test.
				gittest.CommitsByID["ce369011c189f62c815f5971d096b26759bab0d1"],
			},
		},
		{
			desc: "revisions with sort date order",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"master~2",
					"^master~4",
					"flat-path",
					"^flat-path~",
				},
				Order: gitalypb.ListCommitsRequest_DATE,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["60ecb67744cb56576c30214ff52294f8ce2def98"],
				gittest.CommitsByID["55bc176024cfa3baaceb71db584c7e5df900ea65"],
				// This commit is sorted differently compared to the preceding test.
				gittest.CommitsByID["ce369011c189f62c815f5971d096b26759bab0d1"],
				gittest.CommitsByID["e63f41fe459e62e1228fcef60d7189127aeba95a"],
				gittest.CommitsByID["4a24d82dbca5c11c61556f3b35ca472b7463187e"],
			},
		},
		{
			desc: "revision with pseudo-revisions",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
					"--not",
					"--all",
				},
			},
			expectedCommits: nil,
		},
		{
			desc: "only non-merge commits",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				MaxParents: 1,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb"],
				gittest.CommitsByID["48ca272b947f49eee601639d743784a176574a09"],
				gittest.CommitsByID["9d526f87b82e2b2fd231ca44c95508e5e85624ca"],
				gittest.CommitsByID["335bc94d5b7369b10251e612158da2e4a4aaa2a5"],
				gittest.CommitsByID["1039376155a0d507eba0ea95c29f8f5b983ea34b"],
				gittest.CommitsByID["54188278422b1fa877c2e71c4e37fc6640a58ad1"],
				gittest.CommitsByID["8b9270332688d58e25206601900ee5618fab2390"],
				gittest.CommitsByID["f9220df47bce1530e90c189064d301bfc8ceb5ab"],
				gittest.CommitsByID["40d408f89c1fd26b7d02e891568f880afe06a9f8"],
				gittest.CommitsByID["df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"],
				gittest.CommitsByID["6762605237fc246ae146ac64ecb467f71d609120"],
				gittest.CommitsByID["79b06233d3dc769921576771a4e8bee4b439595d"],
				gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
			},
		},
		{
			desc: "disabled walk",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				DisableWalk: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
			},
		},
		{
			desc: "first-parent",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				FirstParent: true,
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["0031876facac3f2b2702a0e53a26e89939a42209"],
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
			},
		},
		{
			desc: "author",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				Author: []byte("Dmitriy"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
			},
		},
		{
			desc: "time range",
			request: &gitalypb.ListCommitsRequest{
				Repository: repo,
				Revisions: []string{
					"0031876facac3f2b2702a0e53a26e89939a42209",
				},
				After: &timestamp.Timestamp{
					Seconds: 1393488197,
				},
				Before: &timestamp.Timestamp{
					Seconds: 1393488199,
				},
			},
			expectedCommits: []*gitalypb.GitCommit{
				gittest.CommitsByID["1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"],
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.ListCommits(ctx, tc.request)
			require.NoError(t, err)

			var commits []*gitalypb.GitCommit
			for {
				response, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					require.Equal(t, tc.expectedErr, err)
				}

				commits = append(commits, response.Commits...)
			}

			testassert.ProtoEqual(t, tc.expectedCommits, commits)
		})
	}
}
