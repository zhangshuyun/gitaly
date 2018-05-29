package commit

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestExtractCommitSignatureSuccess(t *testing.T) {
	exampleSignature, err := ioutil.ReadFile("testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signature")
	require.NoError(t, err)

	exampleSignedText, err := ioutil.ReadFile("testdata/commit-5937ac0a7beb003549fc5fd26fc247adbce4a52e-signed-text")
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		req        *pb.ExtractCommitSignatureRequest
		signature  []byte
		signedText []byte
	}{
		{
			desc: "commit with signature",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: testRepo,
				CommitId:   "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			},
			signature:  exampleSignature,
			signedText: exampleSignedText,
		},
		{
			desc: "commit without signature",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: testRepo,
				CommitId:   "e63f41fe459e62e1228fcef60d7189127aeba95a",
			},
			signature:  nil,
			signedText: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			signature, signedText, err := getSignatureAndText(ctx, client, tc.req)
			require.NoError(t, err)

			require.Equal(t, string(tc.signature), string(signature))
			require.Equal(t, string(tc.signedText), string(signedText))
		})
	}
}

func getSignatureAndText(ctx context.Context, client pb.CommitServiceClient, req *pb.ExtractCommitSignatureRequest) ([]byte, []byte, error) {
	stream, err := client.ExtractCommitSignature(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	var signature, signedText []byte
	var resp *pb.ExtractCommitSignatureResponse
	for err == nil {
		resp, err = stream.Recv()
		if err != nil && err != io.EOF {
			return nil, nil, err
		}

		signature = append(signature, resp.GetSignature()...)
		signedText = append(signedText, resp.GetSignedText()...)
	}

	return signature, signedText, nil
}

func TestExtractCommitSignatureFail(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc string
		req  *pb.ExtractCommitSignatureRequest
		code codes.Code
	}{
		{
			desc: "truncated commit ID",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: testRepo,
				CommitId:   "5937ac0a7beb003549fc5fd26",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit ID",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: testRepo,
				CommitId:   "",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo field",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: nil,
				CommitId:   "e63f41fe459e62e1228fcef60d7189127aeba95a",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "commit ID unknown",
			req: &pb.ExtractCommitSignatureRequest{
				Repository: testRepo,
				CommitId:   "0000000000000000000000000000000000000000",
			},
			code: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ExtractCommitSignature(ctx, tc.req)
			require.NoError(t, err)

			var resp *pb.ExtractCommitSignatureResponse
			for err == nil {
				resp, err = stream.Recv()
				if resp != nil {
					require.Empty(t, resp.Signature, "signature must be empty")
					require.Empty(t, resp.SignedText, "signed text must be empty")
				}
			}

			if tc.code == codes.OK {
				require.Equal(t, io.EOF, err, "expect EOF when there is no error")
			} else {
				testhelper.RequireGrpcError(t, err, tc.code)
			}
		})
	}
}
