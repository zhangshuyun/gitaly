package sidechannel

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func testProxyServer(ctx context.Context, expectEOF bool) (err error) {
	conn, err := OpenSidechannel(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var buf []byte
	if expectEOF {
		buf, err = io.ReadAll(conn)
	} else {
		buf = make([]byte, 5)
		_, err = conn.Read(buf)
	}

	if err != nil {
		return fmt.Errorf("server read: %w", err)
	}

	if string(buf) != "hello" {
		return fmt.Errorf("server: unexpected request: %q", buf)
	}

	if _, err := io.WriteString(conn, "world"); err != nil {
		return fmt.Errorf("server write: %w", err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("server close: %w", err)
	}

	return nil
}

func testProxyClient(closeWrite bool) func(*ClientConn) error {
	return func(conn *ClientConn) (err error) {
		if _, err := io.WriteString(conn, "hello"); err != nil {
			return fmt.Errorf("client write: %w", err)
		}
		if closeWrite {
			if err := conn.CloseWrite(); err != nil {
				return err
			}
		}

		buf, err := io.ReadAll(conn)
		if err != nil {
			return fmt.Errorf("client read: %w", err)
		}
		if string(buf) != "world" {
			return fmt.Errorf("client: unexpected response: %q", buf)
		}

		return nil
	}
}

func TestUnaryProxy(t *testing.T) { testUnaryProxy(t, true) }

func TestUnaryProxy_withoutCloseWrite(t *testing.T) { testUnaryProxy(t, false) }

func testUnaryProxy(t *testing.T, closeWrite bool) {
	upstreamAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			if err := testProxyServer(ctx, closeWrite); err != nil {
				return nil, err
			}
			return &healthpb.HealthCheckResponse{}, nil
		},
	)

	proxyAddr := startServer(
		t,
		func(ctx context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			conn, err := dialProxy(upstreamAddr)
			if err != nil {
				return nil, err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(ctx)
			return healthpb.NewHealthClient(conn).Check(ctxOut, request)
		},
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	conn, registry := dial(t, proxyAddr)
	require.NoError(t, call(ctx, conn, registry, testProxyClient(closeWrite)))
}

func newLogger() *logrus.Entry { return logrus.NewEntry(logrus.New()) }

func dialProxy(upstreamAddr string) (*grpc.ClientConn, error) {
	registry := NewRegistry()
	factory := func() backchannel.Server {
		lm := listenmux.New(insecure.NewCredentials())
		lm.Register(NewServerHandshaker(registry))
		return grpc.NewServer(grpc.Creds(lm))
	}

	clientHandshaker := backchannel.NewClientHandshaker(newLogger(), factory)
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(clientHandshaker.ClientHandshake(insecure.NewCredentials())),
		grpc.WithUnaryInterceptor(NewUnaryProxy(registry)),
		grpc.WithStreamInterceptor(NewStreamProxy(registry)),
	}

	return grpc.Dial(upstreamAddr, dialOpts...)
}

func TestStreamProxy(t *testing.T) { testStreamProxy(t, true) }

func TestStreamProxy_noCloseWrite(t *testing.T) { testStreamProxy(t, false) }

func testStreamProxy(t *testing.T, closeWrite bool) {
	upstreamAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadPackServer) error {
			return testProxyServer(stream.Context(), closeWrite)
		},
	)

	proxyAddr := startStreamServer(
		t,
		func(stream gitalypb.SSHService_SSHUploadPackServer) error {
			conn, err := dialProxy(upstreamAddr)
			if err != nil {
				return err
			}
			defer conn.Close()

			ctxOut := metadata.IncomingToOutgoing(stream.Context())
			client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadPack(ctxOut)
			if err != nil {
				return err
			}

			if _, err := client.Recv(); err != io.EOF {
				return fmt.Errorf("grpc proxy recv: %w", err)
			}

			return nil
		},
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	conn, registry := dial(t, proxyAddr)
	ctx, waiter := RegisterSidechannel(ctx, registry, testProxyClient(closeWrite))
	defer waiter.Close()

	client, err := gitalypb.NewSSHServiceClient(conn).SSHUploadPack(ctx)
	require.NoError(t, err)

	_, err = client.Recv()
	require.Equal(t, io.EOF, err)

	require.NoError(t, waiter.Close())
}

type mockSSHService struct {
	sshUploadPackFunc func(gitalypb.SSHService_SSHUploadPackServer) error
	gitalypb.UnimplementedSSHServiceServer
}

func (m mockSSHService) SSHUploadPack(stream gitalypb.SSHService_SSHUploadPackServer) error {
	return m.sshUploadPackFunc(stream)
}

func startStreamServer(t *testing.T, handler func(gitalypb.SSHService_SSHUploadPackServer) error) string {
	t.Helper()

	lm := listenmux.New(insecure.NewCredentials())
	lm.Register(backchannel.NewServerHandshaker(
		newLogger(), backchannel.NewRegistry(), nil,
	))

	srv := grpc.NewServer(grpc.Creds(lm))
	gitalypb.RegisterSSHServiceServer(srv, &mockSSHService{
		sshUploadPackFunc: handler,
	})

	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	t.Cleanup(srv.Stop)
	go srv.Serve(ln)
	return ln.Addr().String()
}
