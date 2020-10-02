package hook

import (
	"context"
	"crypto/sha1"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type testTransactionServer struct {
	handler func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	if s.handler != nil {
		return s.handler(in)
	}
	return nil, nil
}

func TestReferenceTransactionHookInvalidArgument(t *testing.T) {
	serverSocketPath, stop := runHooksServer(t, config.Config)
	defer stop()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.ReferenceTransactionHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{}))
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestReferenceTransactionHook(t *testing.T) {
	testCases := []struct {
		desc              string
		stdin             []byte
		voteResponse      gitalypb.VoteTransactionResponse_TransactionState
		expectedCode      codes.Code
		expectedReftxHash []byte
	}{
		{
			desc:              "hook triggers transaction",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_COMMIT,
			expectedCode:      codes.OK,
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:              "hook fails with failed vote",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_ABORT,
			expectedCode:      codes.Internal,
			expectedReftxHash: []byte("foobar"),
		},
	}

	transactionServer := &testTransactionServer{}
	grpcServer := grpc.NewServer()
	gitalypb.RegisterRefTransactionServer(grpcServer, transactionServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errQ := make(chan error)
	go func() {
		errQ <- grpcServer.Serve(listener)
	}()
	defer func() {
		grpcServer.Stop()
		require.NoError(t, <-errQ)
	}()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var reftxHash []byte
			transactionServer.handler = func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				reftxHash = in.ReferenceUpdatesHash
				return &gitalypb.VoteTransactionResponse{
					State: tc.voteResponse,
				}, nil
			}

			testRepo, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			serverSocketPath, stop := runHooksServer(t, config.Cfg{})
			defer stop()

			transactionServer := metadata.PraefectServer{
				ListenAddr: "tcp://" + listener.Addr().String(),
			}
			transactionServerEnv, err := transactionServer.Env()
			require.NoError(t, err)

			transaction := metadata.Transaction{
				ID:   1234,
				Node: "node-1",
			}
			transactionEnv, err := transaction.Env()
			require.NoError(t, err)

			environment := []string{
				transactionEnv,
				transactionServerEnv,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			stream, err := client.ReferenceTransactionHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{
				Repository:           testRepo,
				EnvironmentVariables: environment,
			}))
			require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{
				Stdin: tc.stdin,
			}))
			require.NoError(t, stream.CloseSend())

			resp, err := stream.Recv()
			require.Equal(t, helper.GrpcCode(err), tc.expectedCode)
			if tc.expectedCode == codes.OK {
				require.Equal(t, resp.GetExitStatus().GetValue(), int32(0))
			}

			var expectedReftxHash []byte
			if tc.expectedReftxHash != nil {
				hash := sha1.Sum(tc.expectedReftxHash)
				expectedReftxHash = hash[:]
			}
			require.Equal(t, expectedReftxHash[:], reftxHash)
		})
	}
}
