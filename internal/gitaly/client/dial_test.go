package client

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDial(t *testing.T) {
	errNonMuxed := status.Error(codes.Internal, "non-muxed connection")
	errMuxed := status.Error(codes.Internal, "muxed connection")

	logger := testhelper.DiscardTestEntry(t)

	srv := grpc.NewServer(
		grpc.Creds(backchannel.NewServerHandshaker(logger, backchannel.Insecure(), backchannel.NewRegistry(), nil)),
		grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
			_, err := backchannel.GetPeerID(stream.Context())
			if err == backchannel.ErrNonMultiplexedConnection {
				return errNonMuxed
			}

			assert.NoError(t, err)
			return errMuxed
		}),
	)
	defer srv.Stop()

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go srv.Serve(ln)

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("non-muxed conn", func(t *testing.T) {
		nonMuxedConn, err := Dial(ctx, "tcp://"+ln.Addr().String(), nil, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, nonMuxedConn.Close()) }()

		require.Equal(t, errNonMuxed, nonMuxedConn.Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}))
	})

	t.Run("muxed conn", func(t *testing.T) {
		handshaker := backchannel.NewClientHandshaker(logger, func() backchannel.Server { return grpc.NewServer() })
		nonMuxedConn, err := Dial(ctx, "tcp://"+ln.Addr().String(), nil, handshaker)
		require.NoError(t, err)
		defer func() { require.NoError(t, nonMuxedConn.Close()) }()

		require.Equal(t, errMuxed, nonMuxedConn.Invoke(ctx, "/Service/Method", &gitalypb.VoteTransactionRequest{}, &gitalypb.VoteTransactionResponse{}))
	})
}
