package commit

import (
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulGetCommitSignaturesRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &pb.GetCommitSignaturesRequest{
		Repository: testRepo,
		CommitIds: []string{
			"5937ac0a7beb003549fc5fd26fc247adbce4a52e", // has signature
			"e63f41fe459e62e1228fcef60d7189127aeba95a", // has no signature
			"0000000000000000000000000000000000000000", // does not exist
			"a17a9f66543673edf0a3d1c6b93bdda3fe600f32", // has signature
		},
	}

	c, err := client.GetCommitSignatures(ctx, request)
	require.NoError(t, err)

	expectedSignautes := []*pb.GetCommitSignaturesResponse{
		{
			CommitId:   "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			Signature:  testhelper.MustReadFile(t, "testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signed-text"),
		},
		{
			CommitId:   "a17a9f66543673edf0a3d1c6b93bdda3fe600f32",
			Signature:  testhelper.MustReadFile(t, "testdata/gitlab-test-commit-a17a9f66543673edf0a3d1c6b93bdda3fe600f32-signature"),
			SignedText: testhelper.MustReadFile(t, "testdata/gitlab-test-commit-a17a9f66543673edf0a3d1c6b93bdda3fe600f32-signed-text"),
		},
	}
	fetchedSignatures := readAllSignaturesFromClient(t, c)

	require.Equal(t, expectedSignautes, fetchedSignatures)
}

func TestFailedGetCommitSignaturesRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *pb.GetCommitSignaturesRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &pb.GetCommitSignaturesRequest{
				Repository: nil,
				CommitIds:  []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e"},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty CommitIds",
			request: &pb.GetCommitSignaturesRequest{
				Repository: testRepo,
				CommitIds:  []string{},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.GetCommitSignatures(ctx, testCase.request)
			require.NoError(t, err)

			for {
				_, err = c.Recv()
				if err != nil {
					break
				}
			}

			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func readAllSignaturesFromClient(t *testing.T, c pb.CommitService_GetCommitSignaturesClient) (signatures []*pb.GetCommitSignaturesResponse) {
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if resp.CommitId != "" {
			signatures = append(signatures, resp)
			// first message contains either Signature or SignedText so no need to append anything
			continue
		}

		currentSignature := signatures[len(signatures)-1]
		if len(resp.Signature) != 0 {
			currentSignature.Signature = append(currentSignature.Signature, resp.Signature...)
		} else if len(resp.SignedText) != 0 {
			currentSignature.SignedText = append(currentSignature.SignedText, resp.SignedText...)
		}
	}

	return
}
