package proxy_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	testservice "gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/testdata"
)

// TestStreamPeeking demonstrates that a director function is able to peek
// into a stream. Further more, it demonstrates that peeking into a stream
// will not disturb the stream sent from the proxy client to the backend.
func TestStreamPeeking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	backendCC, backendSrvr, cleanupPinger := newBackendPinger(t, ctx)
	defer cleanupPinger()

	pingReqSent := &testservice.PingRequest{Value: "hi"}

	// director will peek into stream before routing traffic
	director := func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
		t.Logf("director routing method %s to backend", fullMethodName)

		peekedMsg, err := peeker.Peek()
		require.NoError(t, err)

		peekedRequest := &testservice.PingRequest{}
		err = proto.Unmarshal(peekedMsg, peekedRequest)
		require.NoError(t, err)
		require.True(t, proto.Equal(pingReqSent, peekedRequest), "expected to be the same")

		return proxy.NewStreamParameters(proxy.Destination{Ctx: helper.IncomingToOutgoing(ctx), Conn: backendCC, Msg: peekedMsg}, nil, nil, nil), nil
	}

	pingResp := &testservice.PingResponse{
		Counter: 1,
	}

	// we expect the backend server to receive the peeked message
	backendSrvr.pingStream = func(stream testservice.TestService_PingStreamServer) error {
		pingReqReceived, err := stream.Recv()
		assert.NoError(t, err)
		assert.True(t, proto.Equal(pingReqSent, pingReqReceived), "expected to be the same")

		return stream.Send(pingResp)
	}

	proxyCC, cleanupProxy := newProxy(t, ctx, director, "mwitkow.testproto.TestService", "PingStream")
	defer cleanupProxy()

	proxyClient := testservice.NewTestServiceClient(proxyCC)

	proxyClientPingStream, err := proxyClient.PingStream(ctx)
	require.NoError(t, err)
	defer proxyClientPingStream.CloseSend()

	require.NoError(t,
		proxyClientPingStream.Send(pingReqSent),
	)

	resp, err := proxyClientPingStream.Recv()
	require.NoError(t, err)
	require.True(t, proto.Equal(resp, pingResp), "expected to be the same")

	_, err = proxyClientPingStream.Recv()
	require.Error(t, io.EOF, err)
}

func TestStreamInjecting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	backendCC, backendSrvr, cleanupPinger := newBackendPinger(t, ctx)
	defer cleanupPinger()

	pingReqSent := &testservice.PingRequest{Value: "hi"}
	newValue := "bye"

	// director will peek into stream and change some frames
	director := func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
		t.Logf("modifying request for method %s", fullMethodName)

		peekedMsg, err := peeker.Peek()
		require.NoError(t, err)

		peekedRequest := &testservice.PingRequest{}
		require.NoError(t, proto.Unmarshal(peekedMsg, peekedRequest))
		require.Equal(t, "hi", peekedRequest.GetValue())

		peekedRequest.Value = newValue

		newPayload, err := proto.Marshal(peekedRequest)
		require.NoError(t, err)

		return proxy.NewStreamParameters(proxy.Destination{Ctx: helper.IncomingToOutgoing(ctx), Conn: backendCC, Msg: newPayload}, nil, nil, nil), nil
	}

	pingResp := &testservice.PingResponse{
		Counter: 1,
	}

	// we expect the backend server to receive the modified message
	backendSrvr.pingStream = func(stream testservice.TestService_PingStreamServer) error {
		pingReqReceived, err := stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, newValue, pingReqReceived.GetValue())

		return stream.Send(pingResp)
	}

	proxyCC, cleanupProxy := newProxy(t, ctx, director, "mwitkow.testproto.TestService", "PingStream")
	defer cleanupProxy()

	proxyClient := testservice.NewTestServiceClient(proxyCC)

	proxyClientPingStream, err := proxyClient.PingStream(ctx)
	require.NoError(t, err)
	defer proxyClientPingStream.CloseSend()

	require.NoError(t,
		proxyClientPingStream.Send(pingReqSent),
	)

	resp, err := proxyClientPingStream.Recv()
	require.NoError(t, err)
	require.True(t, proto.Equal(resp, pingResp), "expected to be the same")

	_, err = proxyClientPingStream.Recv()
	require.Error(t, io.EOF, err)
}
