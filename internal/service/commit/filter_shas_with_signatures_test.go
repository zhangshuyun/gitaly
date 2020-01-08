package commit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestFilterShasWithSignaturesSuccessful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	type testCase struct {
		desc string
		in   [][]byte
		out  [][]byte
	}

	testCases := []testCase{
		{
			desc: "3 shas, none signed",
			in:   [][]byte{[]byte("6907208d755b60ebeacb2e9dfea74c92c3449a1f"), []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"), []byte("d59c60028b053793cecfb4022de34602e1a9218e")},
			out:  nil,
		},
		{
			desc: "3 shas, all signed",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("570e7b2abdd848b95f2f578043fc23bd6f6fd24d"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("570e7b2abdd848b95f2f578043fc23bd6f6fd24d"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
		{
			desc: "3 shas, middle unsigned",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("66eceea0db202bb39c4e445e8ca28689645366c5"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
		{
			desc: "3 shas, middle non-existent",
			in:   [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("deadf00d00000000000000000000000000000000"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
			out:  [][]byte{[]byte("5937ac0a7beb003549fc5fd26fc247adbce4a52e"), []byte("6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.FilterShasWithSignatures(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.FilterShasWithSignaturesRequest{Repository: testRepo, Shas: tc.in}))
			require.NoError(t, stream.CloseSend())
			recvOut, err := recvFSWS(stream)
			require.NoError(t, err)
			require.Equal(t, tc.out, recvOut)
		})
	}
}

func TestFilterShasWithSignaturesValidationError(t *testing.T) {
	err := validateFirstFilterShasWithSignaturesRequest(&gitalypb.FilterShasWithSignaturesRequest{})
	require.Contains(t, err.Error(), "empty Repository")
}

func recvFSWS(stream gitalypb.CommitService_FilterShasWithSignaturesClient) ([][]byte, error) {
	var ret [][]byte
	resp, err := stream.Recv()
	for ; err == nil; resp, err = stream.Recv() {
		ret = append(ret, resp.GetShas()...)
	}
	if err != io.EOF {
		return nil, err
	}
	return ret, nil
}
