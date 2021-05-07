package hook

import (
	"context"
	"crypto/sha1"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	handler func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	if s.handler != nil {
		return s.handler(in)
	}
	return nil, nil
}

func TestReferenceTransactionHookInvalidArgument(t *testing.T) {
	cfg := testcfg.Build(t)
	serverSocketPath := runHooksServer(t, cfg, nil)

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
		state             gitalypb.ReferenceTransactionHookRequest_State
		voteResponse      gitalypb.VoteTransactionResponse_TransactionState
		expectedCode      codes.Code
		expectedReftxHash []byte
	}{
		{
			desc:              "hook triggers transaction with default state",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_COMMIT,
			expectedCode:      codes.OK,
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:              "hook triggers transaction with explicit prepared state",
			stdin:             []byte("foobar"),
			state:             gitalypb.ReferenceTransactionHookRequest_PREPARED,
			voteResponse:      gitalypb.VoteTransactionResponse_COMMIT,
			expectedCode:      codes.OK,
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:         "hook does not trigger transaction with aborted state",
			stdin:        []byte("foobar"),
			state:        gitalypb.ReferenceTransactionHookRequest_ABORTED,
			expectedCode: codes.OK,
		},
		{
			desc:         "hook does not trigger transaction with committed state",
			stdin:        []byte("foobar"),
			state:        gitalypb.ReferenceTransactionHookRequest_COMMITTED,
			expectedCode: codes.OK,
		},
		{
			desc:              "hook fails with failed vote",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_ABORT,
			expectedCode:      codes.Aborted,
			expectedReftxHash: []byte("foobar"),
		},
		{
			desc:              "hook fails with stopped vote",
			stdin:             []byte("foobar"),
			voteResponse:      gitalypb.VoteTransactionResponse_STOP,
			expectedCode:      codes.FailedPrecondition,
			expectedReftxHash: []byte("foobar"),
		},
	}

	transactionServer := &testTransactionServer{}
	grpcServer := grpc.NewServer()
	gitalypb.RegisterRefTransactionServer(grpcServer, transactionServer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	backchannelConn, err := grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	defer backchannelConn.Close()

	registry := backchannel.NewRegistry()
	backchannelID := registry.RegisterBackchannel(backchannelConn)

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
			cfg, repo, _ := testcfg.BuildWithRepo(t)

			var reftxHash []byte
			transactionServer.handler = func(in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				reftxHash = in.ReferenceUpdatesHash
				return &gitalypb.VoteTransactionResponse{
					State: tc.voteResponse,
				}, nil
			}

			serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithBackchannelRegistry(registry))

			ctx, cancel := testhelper.Context()
			defer cancel()

			hooksPayload, err := git.NewHooksPayload(
				cfg,
				repo,
				&txinfo.Transaction{
					ID:   1234,
					Node: "node-1",
				},
				&txinfo.PraefectServer{BackchannelID: backchannelID},
				nil,
				git.ReferenceTransactionHook,
				featureflag.RawFromContext(ctx),
			).Env()
			require.NoError(t, err)

			environment := []string{
				hooksPayload,
			}

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			stream, err := client.ReferenceTransactionHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.ReferenceTransactionHookRequest{
				Repository:           repo,
				State:                tc.state,
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
