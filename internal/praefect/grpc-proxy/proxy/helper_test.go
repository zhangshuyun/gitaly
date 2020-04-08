package proxy_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	testservice "gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/testdata"
	"google.golang.org/grpc"
)

func newListener(tb testing.TB) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err, "must be able to allocate a port for listener")

	return listener
}

func newBackendPinger(tb testing.TB, ctx context.Context) (*grpc.ClientConn, *interceptPinger, func()) {
	ip := &interceptPinger{}

	done := make(chan struct{})
	srvr := grpc.NewServer()
	listener := newListener(tb)

	testservice.RegisterTestServiceServer(srvr, ip)

	go func() {
		defer close(done)
		srvr.Serve(listener)
	}()

	cc, err := grpc.DialContext(
		ctx,
		listener.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.ForceCodec(proxy.NewCodec()),
		),
	)
	require.NoError(tb, err)

	cleanup := func() {
		srvr.GracefulStop()
		require.NoError(tb, cc.Close())
		<-done
	}

	return cc, ip, cleanup
}

func newProxy(tb testing.TB, ctx context.Context, director proxy.StreamDirector, svc, method string) (*grpc.ClientConn, func()) {
	proxySrvr := grpc.NewServer(
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	)

	proxy.RegisterService(proxySrvr, director, svc, method)

	done := make(chan struct{})
	listener := newListener(tb)

	go func() {
		defer close(done)
		proxySrvr.Serve(listener)
	}()

	proxyCC, err := grpc.DialContext(
		ctx,
		listener.Addr().String(),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	require.NoError(tb, err)

	cleanup := func() {
		proxySrvr.GracefulStop()
		require.NoError(tb, proxyCC.Close())
		<-done
	}

	return proxyCC, cleanup
}

// interceptPinger allows an RPC to be intercepted with a custom
// function defined in each unit test
type interceptPinger struct {
	pingStream func(testservice.TestService_PingStreamServer) error
	pingEmpty  func(context.Context, *testservice.Empty) (*testservice.PingResponse, error)
	ping       func(context.Context, *testservice.PingRequest) (*testservice.PingResponse, error)
	pingError  func(context.Context, *testservice.PingRequest) (*testservice.Empty, error)
	pingList   func(*testservice.PingRequest, testservice.TestService_PingListServer) error
}

func (ip *interceptPinger) PingStream(stream testservice.TestService_PingStreamServer) error {
	return ip.pingStream(stream)
}

func (ip *interceptPinger) PingEmpty(ctx context.Context, req *testservice.Empty) (*testservice.PingResponse, error) {
	return ip.pingEmpty(ctx, req)
}

func (ip *interceptPinger) Ping(ctx context.Context, req *testservice.PingRequest) (*testservice.PingResponse, error) {
	return ip.ping(ctx, req)
}

func (ip *interceptPinger) PingError(ctx context.Context, req *testservice.PingRequest) (*testservice.Empty, error) {
	return ip.pingError(ctx, req)
}

func (ip *interceptPinger) PingList(req *testservice.PingRequest, stream testservice.TestService_PingListServer) error {
	return ip.pingList(req, stream)
}
