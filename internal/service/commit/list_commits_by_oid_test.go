package commit

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestSuccessfulListCommitsByOidRequest(t *testing.T) {
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
		request         *gitalypb.ListCommitsByOidRequest
		expectedCommits []*gitalypb.GitCommit
	}{
		{
			desc: "find one commit",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{commits[0].Id},
			},
			expectedCommits: commits[0:1],
		},
		{
			desc: "find multiple commits",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{commits[0].Id, commits[1].Id},
			},
			expectedCommits: commits,
		},
		{
			desc: "no query",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{},
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "empty query",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{""},
			},
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			desc: "partial oids",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{commits[0].Id[0:10], commits[1].Id[0:8]},
			},
			expectedCommits: commits,
		},
		{
			desc: "unknown oids",
			request: &gitalypb.ListCommitsByOidRequest{
				Oid: []string{"deadbeef", "987654321"},
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
			c, err := client.ListCommitsByOid(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			receivedCommits := consumeGetByOidResponse(t, c)

			require.Equal(t, len(testCase.expectedCommits), len(receivedCommits), "number of commits received")

			for i, receivedCommit := range receivedCommits {
				require.Equal(t, testCase.expectedCommits[i], receivedCommit, "mismatched commit")
			}
		})
	}
}

func consumeGetByOidResponse(t *testing.T, c gitalypb.CommitService_ListCommitsByOidClient) []*gitalypb.GitCommit {
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

var masterCommitids = []string{
	"7975be0116940bf2ad4321f79d02a55c5f7779aa",
	"c84ff944ff4529a70788a5e9003c2b7feae29047",
	"60ecb67744cb56576c30214ff52294f8ce2def98",
	"55bc176024cfa3baaceb71db584c7e5df900ea65",
	"e63f41fe459e62e1228fcef60d7189127aeba95a",
	"4a24d82dbca5c11c61556f3b35ca472b7463187e",
	"b83d6e391c22777fca1ed3012fce84f633d7fed0",
	"498214de67004b1da3d820901307bed2a68a8ef6",
	"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
	"38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
	"6907208d755b60ebeacb2e9dfea74c92c3449a1f",
	"c347ca2e140aa667b968e51ed0ffe055501fe4f4",
	"d59c60028b053793cecfb4022de34602e1a9218e",
	"281d3a76f31c812dbf48abce82ccf6860adedd81",
	"a5391128b0ef5d21df5dd23d98557f4ef12fae20",
	"54fcc214b94e78d7a41a9a8fe6d87a5e59500e51",
	"be93687618e4b132087f430a4d8fc3a609c9b77c",
	"048721d90c449b244b7b4c53a9186b04330174ec",
	"5f923865dde3436854e9ceb9cdb7815618d4e849",
	"d2d430676773caa88cdaf7c55944073b2fd5561a",
	"2ea1f3dec713d940208fb5ce4a38765ecb5d3f73",
	"59e29889be61e6e0e5e223bfa9ac2721d31605b8",
	"66eceea0db202bb39c4e445e8ca28689645366c5",
	"08f22f255f082689c0d7d39d19205085311542bc",
	"19e2e9b4ef76b422ce1154af39a91323ccc57434",
	"c642fe9b8b9f28f9225d7ea953fe14e74748d53b",
	"9a944d90955aaf45f6d0c88f30e27f8d2c41cec0",
	"c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd",
	"e56497bb5f03a90a51293fc6d516788730953899",
	"4cd80ccab63c82b4bad16faa5193fbd2aa06df40",
	"5937ac0a7beb003549fc5fd26fc247adbce4a52e",
	"570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
	"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
	"d14d6c0abdd253381df51a723d58691b2ee1ab08",
	"c1acaa58bbcbc3eafe538cb8274ba387047b69f8",
	"ae73cb07c9eeaf35924a10f713b364d32b2dd34f",
	"874797c3a73b60d2187ed6e2fcabd289ff75171e",
	"2f63565e7aac07bcdadb654e253078b727143ec4",
	"33f3729a45c02fc67d00adb1b8bca394b0e761d9",
	"913c66a37b4a45b9769037c55c2d238bd0942d2e",
	"cfe32cf61b73a0d5e9f13e774abde7ff789b1660",
	"6d394385cf567f80a8fd85055db1ab4c5295806f",
	"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
}

func TestSuccessfulListCommitsByOidLargeRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	req := &gitalypb.ListCommitsByOidRequest{
		Oid:        masterCommitids,
		Repository: testRepo,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	c, err := client.ListCommitsByOid(ctx, req)
	require.NoError(t, err)

	actualCommits := consumeGetByOidResponse(t, c)

	require.Equal(t, len(masterCommitids), len(actualCommits))
	for i, actual := range actualCommits {
		require.Equal(t, masterCommitids[i], actual.Id, "commit ID must match, entry %d", i)
	}
}
