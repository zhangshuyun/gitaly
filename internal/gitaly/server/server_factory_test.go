package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/teststream"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestGitalyServerFactory(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("insecure over TCP", func(t *testing.T) {
		cfg, repo, _ := testcfg.BuildWithRepo(t)
		sf := NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))

		check(t, ctx, sf, cfg, repo, starter.TCP, "localhost:0")
	})

	t.Run("insecure over Unix Socket", func(t *testing.T) {
		cfg, repo, _ := testcfg.BuildWithRepo(t)
		sf := NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))

		check(t, ctx, sf, cfg, repo, starter.Unix, testhelper.GetTemporaryGitalySocketFileName(t))
	})

	t.Run("secure", func(t *testing.T) {
		certFile, keyFile := testhelper.GenerateCerts(t)

		cfg, repo, _ := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{TLS: config.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		}}))

		sf := NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))
		t.Cleanup(sf.Stop)

		check(t, ctx, sf, cfg, repo, starter.TLS, "localhost:0")
	})

	t.Run("all services must be stopped", func(t *testing.T) {
		cfg, repo, _ := testcfg.BuildWithRepo(t)
		sf := NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))
		t.Cleanup(sf.Stop)

		tcpHealthClient := check(t, ctx, sf, cfg, repo, starter.TCP, "localhost:0")

		socket := testhelper.GetTemporaryGitalySocketFileName(t)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(socket)) })

		socketHealthClient := check(t, ctx, sf, cfg, repo, starter.Unix, socket)

		sf.GracefulStop() // stops all started servers(listeners)

		_, tcpErr := tcpHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(tcpErr))

		_, socketErr := socketHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(socketErr))
	})
}

func TestGitalyServerFactory_closeOrder(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)
	sf := NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))
	defer sf.Stop()

	errQuickRPC := status.Error(codes.Internal, "quick RPC")
	errBlockingRPC := status.Error(codes.Internal, "blocking RPC")

	invokeQuick := func(conn *grpc.ClientConn, shouldSucceed bool) {
		err := conn.Invoke(ctx, "/Service/Quick", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{})
		if !shouldSucceed {
			testhelper.RequireGrpcError(t, err, codes.Unavailable)
			return
		}

		require.Equal(t, errQuickRPC, err)
	}

	invokeBlocking := func(conn *grpc.ClientConn) chan struct{} {
		rpcFinished := make(chan struct{})

		go func() {
			defer close(rpcFinished)
			assert.Equal(t,
				errBlockingRPC,
				conn.Invoke(ctx, "/Service/Blocking", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{}),
			)
		}()

		return rpcFinished
	}

	waitUntilFailure := func(conn *grpc.ClientConn) {
		for {
			err := conn.Invoke(ctx, "/Service/Quick", &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckRequest{})
			if errors.Is(err, errQuickRPC) {
				continue
			}

			testhelper.RequireGrpcError(t, err, codes.Unavailable)
			break
		}
	}

	var internalConn, externalConn *grpc.ClientConn
	var internalIsBlocking, externalIsBlocking chan struct{}
	var releaseInternalBlock, releaseExternalBlock chan struct{}
	for _, builder := range []struct {
		createServer func() *grpc.Server
		conn         **grpc.ClientConn
		isBlocking   *chan struct{}
		releaseBlock *chan struct{}
	}{
		{
			createServer: func() *grpc.Server {
				server, _, err := sf.CreateInternal()
				require.NoError(t, err)
				return server
			},
			conn:         &internalConn,
			isBlocking:   &internalIsBlocking,
			releaseBlock: &releaseInternalBlock,
		},
		{
			createServer: func() *grpc.Server {
				server, _, err := sf.CreateExternal(false)
				require.NoError(t, err)
				return server
			},
			conn:         &externalConn,
			isBlocking:   &externalIsBlocking,
			releaseBlock: &releaseExternalBlock,
		},
	} {
		server := builder.createServer()

		releaseBlock := make(chan struct{})
		*builder.releaseBlock = releaseBlock

		isBlocking := make(chan struct{})
		*builder.isBlocking = isBlocking

		server.RegisterService(&grpc.ServiceDesc{
			ServiceName: "Service",
			Methods: []grpc.MethodDesc{
				{
					MethodName: "Quick",
					Handler: func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error) {
						return nil, errQuickRPC
					},
				},
				{
					MethodName: "Blocking",
					Handler: func(interface{}, context.Context, func(interface{}) error, grpc.UnaryServerInterceptor) (interface{}, error) {
						close(isBlocking)
						<-releaseBlock
						return nil, errBlockingRPC
					},
				},
			},
			HandlerType: (*interface{})(nil),
		}, server)

		ln, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		defer ln.Close()

		go server.Serve(ln)

		*builder.conn, err = grpc.DialContext(ctx, ln.Addr().String(), grpc.WithInsecure())
		require.NoError(t, err)
	}

	// both servers should be up and accepting RPCs
	invokeQuick(externalConn, true)
	invokeQuick(internalConn, true)

	// invoke a blocking RPC on the external server to block the graceful shutdown
	invokeBlocking(externalConn)
	<-externalIsBlocking

	shutdownCompeleted := make(chan struct{})
	go func() {
		defer close(shutdownCompeleted)
		sf.GracefulStop()
	}()

	// wait until the graceful shutdown is in progress and new RPCs are no longer accepted on the
	// external servers
	waitUntilFailure(externalConn)

	// internal sockets should still accept RPCs even if external sockets are gracefully closing.
	invokeQuick(internalConn, true)

	// block on the internal server
	internalBlockingRPCFinished := invokeBlocking(internalConn)
	<-internalIsBlocking

	// release the external server's blocking RPC so the graceful shutdown can complete and proceed to
	// shutting down the internal servers.
	close(releaseExternalBlock)

	// wait until the graceful shutdown is in progress and new RPCs are no longer accepted on the internal
	// servers
	waitUntilFailure(internalConn)

	// neither internal nor external servers should be accepting new RPCs anymore
	invokeQuick(externalConn, false)
	invokeQuick(internalConn, false)

	// wait until the blocking rpc has successfully completed
	close(releaseInternalBlock)
	<-internalBlockingRPCFinished

	// wait until the graceful shutdown completes
	<-shutdownCompeleted
}

func check(t *testing.T, ctx context.Context, sf *GitalyServerFactory, cfg config.Cfg, repo *gitalypb.Repository, schema, addr string) healthpb.HealthClient {
	t.Helper()

	var grpcConn *grpc.ClientConn
	var streamRPCDial streamrpc.DialFunc

	if schema == starter.TLS {
		listener, err := net.Listen(starter.TCP, addr)
		require.NoError(t, err)
		t.Cleanup(func() { listener.Close() })

		grpcSrv, srpcSrv, err := sf.CreateExternal(true)
		require.NoError(t, err)
		healthpb.RegisterHealthServer(grpcSrv, health.NewServer())
		registerStreamRPCServers(t, srpcSrv, cfg)
		go grpcSrv.Serve(listener)

		certPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		pem := testhelper.MustReadFile(t, sf.cfg.TLS.CertPath)
		require.True(t, certPool.AppendCertsFromPEM(pem))

		tlsConf := &tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		}
		creds := credentials.NewTLS(tlsConf)

		streamRPCDial = streamrpc.DialTLS(listener.Addr().String(), tlsConf)
		grpcConn, err = grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
	} else {
		listener, err := net.Listen(schema, addr)
		require.NoError(t, err)
		t.Cleanup(func() { listener.Close() })

		grpcSrv, srpcSrv, err := sf.CreateExternal(false)
		require.NoError(t, err)
		healthpb.RegisterHealthServer(grpcSrv, health.NewServer())
		registerStreamRPCServers(t, srpcSrv, cfg)
		go grpcSrv.Serve(listener)

		endpoint, err := starter.ComposeEndpoint(schema, listener.Addr().String())
		require.NoError(t, err)

		streamRPCDial = streamrpc.DialNet(endpoint)
		grpcConn, err = client.Dial(endpoint, nil)
		require.NoError(t, err)
	}

	// Make a healthcheck gRPC call
	t.Cleanup(func() { grpcConn.Close() })
	healthClient := healthpb.NewHealthClient(grpcConn)

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)

	// Make a streamRPC call
	in, out, err := checkStreamRPC(t, streamRPCDial, repo)
	require.NoError(t, err)
	require.Equal(t, in, out, "byte stream works")

	return healthClient
}

func registerStreamRPCServers(t *testing.T, srv *streamrpc.Server, cfg config.Cfg) {
	gitalypb.RegisterTestStreamServiceServer(srv, teststream.NewServer(config.NewLocator(cfg)))
}

func checkStreamRPC(t *testing.T, dial streamrpc.DialFunc, repo *gitalypb.Repository, opts ...streamrpc.CallOption) ([]byte, []byte, error) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	const size = 1024 * 1024

	in := make([]byte, size)
	_, err := rand.Read(in)
	require.NoError(t, err)

	var out []byte
	require.NotEqual(t, in, out)

	err = streamrpc.Call(
		ctx,
		dial,
		"/gitaly.TestStreamService/TestStream",
		&gitalypb.TestStreamRequest{
			Repository: repo,
			Size:       size,
		},
		func(c net.Conn) error {
			errC := make(chan error, 1)
			go func() {
				var err error
				out, err = ioutil.ReadAll(c)
				errC <- err
			}()

			if _, err := io.Copy(c, bytes.NewReader(in)); err != nil {
				return err
			}
			if err := <-errC; err != nil {
				return err
			}

			return nil
		},
		opts...,
	)
	return in, out, err
}
