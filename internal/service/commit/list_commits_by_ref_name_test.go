package commit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestSuccessfulListCommitsByRefNameRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc        string
		request     *gitalypb.ListCommitsByRefNameRequest
		expectedIds []string
	}{
		{
			desc: "find one commit",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("refs/heads/master")},
			},
			expectedIds: []string{"1e292f8fedd741b75372e19097c76d327140c312"},
		},
		{
			desc: "find one commit without refs/heads/ prefix",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("master")},
			},
			expectedIds: []string{"1e292f8fedd741b75372e19097c76d327140c312"},
		},
		{
			desc: "find HEAD commit",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("HEAD")},
			},
			expectedIds: []string{"1e292f8fedd741b75372e19097c76d327140c312"},
		},
		{
			desc: "find one commit with UTF8 characters",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("refs/heads/ʕ•ᴥ•ʔ")},
			},
			expectedIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
		},
		{
			desc: "find one commit with non UTF8 characters",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("refs/heads/Ääh-test-utf-8")},
			},
			expectedIds: []string{"7975be0116940bf2ad4321f79d02a55c5f7779aa"},
		},
		{
			desc: "find multiple commits",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{
					[]byte("refs/heads/master"),
					[]byte("refs/heads/add-pdf-file"),
				},
			},
			expectedIds: []string{
				"1e292f8fedd741b75372e19097c76d327140c312",
				"e774ebd33ca5de8e6ef1e633fd887bb52b9d0a7a",
			},
		},
		{
			desc: "unknown ref names",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{
					[]byte("refs/heads/does-not-exist-1"),
					[]byte("refs/heads/does-not-exist-2"),
				},
			},
			expectedIds: []string{},
		},
		{
			desc: "find partial commits",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{
					[]byte("refs/heads/master"),
					[]byte("refs/heads/does-not-exist-1"),
					[]byte("refs/heads/add-pdf-file"),
				},
			},
			expectedIds: []string{
				"1e292f8fedd741b75372e19097c76d327140c312",
				"e774ebd33ca5de8e6ef1e633fd887bb52b9d0a7a",
			},
		},
		{
			desc: "no query",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{},
			},
			expectedIds: []string{},
		},
		{
			desc: "empty query",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("")},
			},
			expectedIds: []string{},
		},
		{
			desc: "find empty commit",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{[]byte("refs/heads/1942eed5cc108b19c7405106e81fa96125d0be22")},
			},
			expectedIds: []string{"1942eed5cc108b19c7405106e81fa96125d0be22"},
		},
		{
			desc: "invalid ref names",
			request: &gitalypb.ListCommitsByRefNameRequest{
				RefNames: [][]byte{
					[]byte("refs/does-not-exist-1"),
					[]byte("does-not-exist-2"),
				},
			},
			expectedIds: []string{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := testCase.request
			request.Repository = testRepo

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.ListCommitsByRefName(ctx, request)
			require.NoError(t, err)

			receivedCommits := consumeGetByRefNameResponse(t, c)
			require.Len(t, receivedCommits, len(testCase.expectedIds))

			for i, receivedCommit := range receivedCommits {
				require.Equal(t, testCase.expectedIds[i], receivedCommit.Id, "mismatched commit")
			}
		})
	}
}

func consumeGetByRefNameResponse(t *testing.T, c gitalypb.CommitService_ListCommitsByRefNameClient) []*gitalypb.GitCommit {
	receivedCommits := []*gitalypb.GitCommit{}
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		receivedCommits = append(receivedCommits, resp.GetCommits()...)
	}

	return receivedCommits
}

var repositoryRefNames = map[string]string{
	"bb5206fee213d983da88c47f9cf4cc6caf9c66dc": "refs/heads/feature_conflict",
	"0031876facac3f2b2702a0e53a26e89939a42209": "refs/heads/few-commits",
	"06041ab2037429d243a38abb55957818dd9f948d": "refs/heads/file-mode-change",
	"48f0be4bd10c1decee6fae52f9ae6d10f77b60f4": "refs/heads/fix",
	"ce369011c189f62c815f5971d096b26759bab0d1": "refs/heads/flat-path",
	"d25b6d94034242f3930dfcfeb6d8d9aac3583992": "refs/heads/flat-path-2",
	"e56497bb5f03a90a51293fc6d516788730953899": "refs/heads/flatten-dirs",
	"ab2c9622c02288a2bbaaf35d96088cfdff31d9d9": "refs/heads/gitaly-diff-stuff",
	"0999bb770f8dc92ab5581cc0b474b3e31a96bf5c": "refs/heads/gitaly-non-utf8-commit",
	"94bb47ca1297b7b3731ff2a36923640991e9236f": "refs/heads/gitaly-rename-test",
	"cb19058ecc02d01f8e4290b7e79cafd16a8839b6": "refs/heads/gitaly-stuff",
	"e63f41fe459e62e1228fcef60d7189127aeba95a": "refs/heads/gitaly-test-ref",
	"c809470461118b7bcab850f6e9a7ca97ac42f8ea": "refs/heads/gitaly-windows-1251",
	"5937ac0a7beb003549fc5fd26fc247adbce4a52e": "refs/heads/improve/awesome",
	"7df99c9ad5b8c9bfc5ae4fb7a91cc87adcce02ef": "refs/heads/jv-conflict-1",
	"bd493d44ae3c4dd84ce89cb75be78c4708cbd548": "refs/heads/jv-conflict-2",
	"d23bddc916b96c98ff192e198b1adee0f6871085": "refs/heads/many_files",
	"0ed8c6c6752e8c6ea63e7b92a517bf5ac1209c80": "refs/heads/markdown",
	"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9": "refs/tags/v1.0.0",
	"8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b": "refs/tags/v1.1.0",
}

func TestSuccessfulListCommitsByRefNameLargeRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	refNames := [][]byte{}
	for _, refName := range repositoryRefNames {
		refNames = append(refNames, []byte(refName))
	}
	req := &gitalypb.ListCommitsByRefNameRequest{
		RefNames:   refNames,
		Repository: testRepo,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.ListCommitsByRefName(ctx, req)
	require.NoError(t, err)

	actualCommits := consumeGetByRefNameResponse(t, c)

	for _, actual := range actualCommits {
		_, ok := repositoryRefNames[actual.Id]
		require.True(t, ok, "commit ID must be present in the input list: %s", actual.Id)
	}
}
