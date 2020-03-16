package ref

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListNewCommits(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	oid := "0031876facac3f2b2702a0e53a26e89939a42209"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-D", "few-commits")

	testCases := []struct {
		revision      string
		newCommitOids []string
		responseCode  codes.Code
	}{
		{
			revision: oid,
			newCommitOids: []string{
				"0031876facac3f2b2702a0e53a26e89939a42209",
				"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
				"48ca272b947f49eee601639d743784a176574a09",
				"9d526f87b82e2b2fd231ca44c95508e5e85624ca",
				"335bc94d5b7369b10251e612158da2e4a4aaa2a5",
				"1039376155a0d507eba0ea95c29f8f5b983ea34b",
				"54188278422b1fa877c2e71c4e37fc6640a58ad1",
				"8b9270332688d58e25206601900ee5618fab2390",
				"f9220df47bce1530e90c189064d301bfc8ceb5ab",
				"40d408f89c1fd26b7d02e891568f880afe06a9f8",
				"df914c609a1e16d7d68e4a61777ff5d6f6b6fde3",
				"6762605237fc246ae146ac64ecb467f71d609120",
				"79b06233d3dc769921576771a4e8bee4b439595d",
			},
		},
		{
			revision:     "- rm -rf /",
			responseCode: codes.InvalidArgument,
		},
		{
			revision:     "1234deadbeef",
			responseCode: codes.InvalidArgument,
		},
		{
			revision:      "7975be0116940bf2ad4321f79d02a55c5f7779aa",
			newCommitOids: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.revision, func(t *testing.T) {
			request := &gitalypb.ListNewCommitsRequest{Repository: testRepo, CommitId: tc.revision}

			stream, err := client.ListNewCommits(ctx, request)
			require.NoError(t, err)

			var commits []*gitalypb.GitCommit
			for {
				msg, err := stream.Recv()

				if err == io.EOF {
					break
				}
				if err != nil {
					require.Equal(t, tc.responseCode, status.Code(err))
					break
				}

				require.NoError(t, err)
				commits = append(commits, msg.Commits...)
			}
			require.Len(t, commits, len(tc.newCommitOids))
			for i, commit := range commits {
				require.Equal(t, commit.Id, tc.newCommitOids[i])
			}
		})
	}
}
