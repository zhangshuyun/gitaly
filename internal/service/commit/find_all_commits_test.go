package commit

import (
	"context"
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindAllCommitsRequest(t *testing.T) {
	defer func() {
		_findBranchNamesFunc = ref.FindBranchNames
	}()

	_findBranchNamesFunc = func(ctx context.Context, repo *gitalypb.Repository) ([][]byte, error) {
		return [][]byte{
			[]byte("few-commits"),
			[]byte("two-commits"),
		}, nil
	}

	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Commits made on another branch in parallel to the normal commits below.
	// Will be used to test topology ordering.
	alternateCommits := []*gitalypb.GitCommit{
		{
			Id:        "0031876facac3f2b2702a0e53a26e89939a42209",
			Subject:   []byte("Merge branch 'few-commits-4' into few-commits-2"),
			Body:      []byte("Merge branch 'few-commits-4' into few-commits-2\n"),
			Author:    dummyCommitAuthor(1500320762),
			Committer: dummyCommitAuthor(1500320762),
			ParentIds: []string{
				"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
				"48ca272b947f49eee601639d743784a176574a09",
			},
			BodySize: 48,
		},
		{
			Id:        "48ca272b947f49eee601639d743784a176574a09",
			Subject:   []byte("Commit #9 alternate"),
			Body:      []byte("Commit #9 alternate\n"),
			Author:    dummyCommitAuthor(1500320271),
			Committer: dummyCommitAuthor(1500320271),
			ParentIds: []string{"335bc94d5b7369b10251e612158da2e4a4aaa2a5"},
			BodySize:  20,
		},
		{
			Id:        "335bc94d5b7369b10251e612158da2e4a4aaa2a5",
			Subject:   []byte("Commit #8 alternate"),
			Body:      []byte("Commit #8 alternate\n"),
			Author:    dummyCommitAuthor(1500320269),
			Committer: dummyCommitAuthor(1500320269),
			ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
			BodySize:  20,
		},
	}

	// Nothing special about these commits.
	normalCommits := []*gitalypb.GitCommit{
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
			Id:        "9d526f87b82e2b2fd231ca44c95508e5e85624ca",
			Subject:   []byte("Commit #9"),
			Body:      []byte("Commit #9\n"),
			Author:    dummyCommitAuthor(1500320270),
			Committer: dummyCommitAuthor(1500320270),
			ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
			BodySize:  10,
		},
		{
			Id:        "1039376155a0d507eba0ea95c29f8f5b983ea34b",
			Subject:   []byte("Commit #8"),
			Body:      []byte("Commit #8\n"),
			Author:    dummyCommitAuthor(1500320268),
			Committer: dummyCommitAuthor(1500320268),
			ParentIds: []string{"54188278422b1fa877c2e71c4e37fc6640a58ad1"},
			BodySize:  10,
		}, {
			Id:        "54188278422b1fa877c2e71c4e37fc6640a58ad1",
			Subject:   []byte("Commit #7"),
			Body:      []byte("Commit #7\n"),
			Author:    dummyCommitAuthor(1500320266),
			Committer: dummyCommitAuthor(1500320266),
			ParentIds: []string{"8b9270332688d58e25206601900ee5618fab2390"},
			BodySize:  10,
		}, {
			Id:        "8b9270332688d58e25206601900ee5618fab2390",
			Subject:   []byte("Commit #6"),
			Body:      []byte("Commit #6\n"),
			Author:    dummyCommitAuthor(1500320264),
			Committer: dummyCommitAuthor(1500320264),
			ParentIds: []string{"f9220df47bce1530e90c189064d301bfc8ceb5ab"},
			BodySize:  10,
		}, {
			Id:        "f9220df47bce1530e90c189064d301bfc8ceb5ab",
			Subject:   []byte("Commit #5"),
			Body:      []byte("Commit #5\n"),
			Author:    dummyCommitAuthor(1500320262),
			Committer: dummyCommitAuthor(1500320262),
			ParentIds: []string{"40d408f89c1fd26b7d02e891568f880afe06a9f8"},
			BodySize:  10,
		}, {
			Id:        "40d408f89c1fd26b7d02e891568f880afe06a9f8",
			Subject:   []byte("Commit #4"),
			Body:      []byte("Commit #4\n"),
			Author:    dummyCommitAuthor(1500320260),
			Committer: dummyCommitAuthor(1500320260),
			ParentIds: []string{"df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"},
			BodySize:  10,
		}, {
			Id:        "df914c609a1e16d7d68e4a61777ff5d6f6b6fde3",
			Subject:   []byte("Commit #3"),
			Body:      []byte("Commit #3\n"),
			Author:    dummyCommitAuthor(1500320258),
			Committer: dummyCommitAuthor(1500320258),
			ParentIds: []string{"6762605237fc246ae146ac64ecb467f71d609120"},
			BodySize:  10,
		}, {
			Id:        "6762605237fc246ae146ac64ecb467f71d609120",
			Subject:   []byte("Commit #2"),
			Body:      []byte("Commit #2\n"),
			Author:    dummyCommitAuthor(1500320256),
			Committer: dummyCommitAuthor(1500320256),
			ParentIds: []string{"79b06233d3dc769921576771a4e8bee4b439595d"},
			BodySize:  10,
		}, {
			Id:        "79b06233d3dc769921576771a4e8bee4b439595d",
			Subject:   []byte("Commit #1"),
			Body:      []byte("Commit #1\n"),
			Author:    dummyCommitAuthor(1500320254),
			Committer: dummyCommitAuthor(1500320254),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
			BodySize:  10,
		},
		{
			Id:      "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			Subject: []byte("Initial commit"),
			Body:    []byte("Initial commit\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393488198},
				Timezone: []byte("-0800"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393488198},
				Timezone: []byte("-0800"),
			},
			ParentIds: nil,
			BodySize:  15,
		},
	}

	// A commit that exists on "two-commits" branch.
	singleCommit := []*gitalypb.GitCommit{
		{
			Id:        "304d257dcb821665ab5110318fc58a007bd104ed",
			Subject:   []byte("Commit #11"),
			Body:      []byte("Commit #11\n"),
			Author:    dummyCommitAuthor(1500322381),
			Committer: dummyCommitAuthor(1500322381),
			ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
			BodySize:  11,
		},
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
			request.Repository = testRepo

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.FindAllCommits(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			receivedCommits := collectCommtsFromFindAllCommitsClient(t, c)

			require.Equal(t, len(testCase.expectedCommits), len(receivedCommits), "number of commits received")

			for i, receivedCommit := range receivedCommits {
				require.Equal(t, testCase.expectedCommits[i], receivedCommit, "mismatched commits")
			}
		})
	}
}

func TestFailedFindAllCommitsRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
				Repository: testRepo,
				Revision:   []byte("--output=/meow"),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.FindAllCommits(ctx, testCase.request)
			if err != nil {
				t.Fatal(err)
			}

			err = drainFindAllCommitsResponse(c)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func collectCommtsFromFindAllCommitsClient(t *testing.T, c gitalypb.CommitService_FindAllCommitsClient) []*gitalypb.GitCommit {
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

	return receivedCommits
}

func drainFindAllCommitsResponse(c gitalypb.CommitService_FindAllCommitsClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
