package commit

import (
	"context"
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitsByMessageRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commits := []*gitalypb.GitCommit{
		{
			Id:        "bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
			Subject:   []byte("Commit #10"),
			Body:      []byte("Commit #10\n"),
			Author:    dummyCommitAuthor(1500320272),
			Committer: dummyCommitAuthor(1500320272),
			ParentIds: []string{"9d526f87b82e2b2fd231ca44c95508e5e85624ca"},
			BodySize:  11,
		},
		{
			Id:        "79b06233d3dc769921576771a4e8bee4b439595d",
			Subject:   []byte("Commit #1"),
			Body:      []byte("Commit #1\n"),
			Author:    dummyCommitAuthor(1500320254),
			Committer: dummyCommitAuthor(1500320254),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
			BodySize:  10,
		},
	}

	testCases := []struct {
		desc            string
		request         *gitalypb.CommitsByMessageRequest
		expectedCommits []*gitalypb.GitCommit
	}{
		{
			desc: "revision + query",
			request: &gitalypb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
			},
			expectedCommits: commits,
		},
		{
			desc: "revision + query + limit",
			request: &gitalypb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
				Limit:    1,
			},
			expectedCommits: commits[0:1],
		},
		{
			desc: "revision + query + offset",
			request: &gitalypb.CommitsByMessageRequest{
				Revision: []byte("few-commits"),
				Query:    "commit #1",
				Offset:   1,
			},
			expectedCommits: commits[1:],
		},
		{
			desc: "query + empty revision + path",
			request: &gitalypb.CommitsByMessageRequest{
				Query: "much more",
				Path:  []byte("files/ruby"),
			},
			expectedCommits: []*gitalypb.GitCommit{
				{
					Id:      "913c66a37b4a45b9769037c55c2d238bd0942d2e",
					Subject: []byte("Files, encoding and much more"),
					Body:    []byte("Files, encoding and much more\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
					Author: &gitalypb.CommitAuthor{
						Name:     []byte("Dmitriy Zaporozhets"),
						Email:    []byte("dmitriy.zaporozhets@gmail.com"),
						Date:     &timestamp.Timestamp{Seconds: 1393488896},
						Timezone: []byte("+0200"),
					},
					Committer: &gitalypb.CommitAuthor{
						Name:     []byte("Dmitriy Zaporozhets"),
						Email:    []byte("dmitriy.zaporozhets@gmail.com"),
						Date:     &timestamp.Timestamp{Seconds: 1393488896},
						Timezone: []byte("+0200"),
					},
					ParentIds:     []string{"cfe32cf61b73a0d5e9f13e774abde7ff789b1660"},
					BodySize:      98,
					SignatureType: gitalypb.SignatureType_PGP,
				},
			},
		},
		{
			desc: "query + empty revision + path not in the commits",
			request: &gitalypb.CommitsByMessageRequest{
				Query: "much more",
				Path:  []byte("bar"),
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "query + bad revision",
			request: &gitalypb.CommitsByMessageRequest{
				Revision: []byte("maaaaasterrrrr"),
				Query:    "much more",
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := testCase.request
			request.Repository = testRepo

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitsByMessage(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			receivedCommits := []*gitalypb.GitCommit{}
			for {
				resp, err := c.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}

				receivedCommits = append(receivedCommits, resp.GetCommits()...)
			}

			require.Equal(t, len(testCase.expectedCommits), len(receivedCommits), "number of commits received")

			for i, receivedCommit := range receivedCommits {
				require.Equal(t, testCase.expectedCommits[i], receivedCommit, "mismatched commit")
			}
		})
	}
}

func TestFailedCommitsByMessageRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.CommitsByMessageRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.CommitsByMessageRequest{Repository: invalidRepo, Query: "foo"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.CommitsByMessageRequest{Query: "foo"},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Query is missing",
			request: &gitalypb.CommitsByMessageRequest{Repository: testRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Revision is invalid",
			request: &gitalypb.CommitsByMessageRequest{Repository: testRepo, Revision: []byte("--output=/meow"), Query: "not empty"},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitsByMessage(ctx, testCase.request)
			if err != nil {
				t.Fatal(err)
			}

			testhelper.RequireGrpcError(t, drainCommitsByMessageResponse(c), testCase.code)
		})
	}
}

func drainCommitsByMessageResponse(c gitalypb.CommitService_CommitsByMessageClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
