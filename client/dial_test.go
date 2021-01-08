package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	proxytestdata "gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/testdata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	gitaly_x509 "gitlab.com/gitlab-org/gitaly/internal/x509"
	"gitlab.com/gitlab-org/labkit/correlation"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var proxyEnvironmentKeys = []string{"http_proxy", "https_proxy", "no_proxy"}

func TestDial(t *testing.T) {
	if emitProxyWarning() {
		t.Log("WARNING. Proxy configuration detected from environment settings. This test failure may be related to proxy configuration. Please process with caution")
	}

	stop, connectionMap, err := startListeners()
	require.NoError(t, err, "start listeners: %v. %s", err)
	defer stop()

	unixSocketAbsPath := connectionMap["unix"]

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	unixSocketPath := filepath.Join(tempDir, "gitaly.socket")
	require.NoError(t, err)
	require.NoError(t, os.Symlink(unixSocketAbsPath, unixSocketPath))

	tests := []struct {
		name                string
		rawAddress          string
		envSSLCertFile      string
		dialOpts            []grpc.DialOption
		expectDialFailure   bool
		expectHealthFailure bool
	}{
		{
			name:                "tcp localhost with prefix",
			rawAddress:          "tcp://localhost:" + connectionMap["tcp"], // "tcp://localhost:1234"
			expectDialFailure:   false,
			expectHealthFailure: false,
		},
		{
			name:                "tls localhost",
			rawAddress:          "tls://localhost:" + connectionMap["tls"], // "tls://localhost:1234"
			envSSLCertFile:      "./testdata/gitalycert.pem",
			expectDialFailure:   false,
			expectHealthFailure: false,
		},
		{
			name:                "unix absolute",
			rawAddress:          "unix:" + unixSocketAbsPath, // "unix:/tmp/temp-socket"
			expectDialFailure:   false,
			expectHealthFailure: false,
		},
		{
			name:                "unix relative",
			rawAddress:          "unix:" + unixSocketPath, // "unix:../../tmp/temp-socket"
			expectDialFailure:   false,
			expectHealthFailure: false,
		},
		{
			name:                "unix absolute does not exist",
			rawAddress:          "unix:" + unixSocketAbsPath + ".does_not_exist", // "unix:/tmp/temp-socket.does_not_exist"
			expectDialFailure:   false,
			expectHealthFailure: true,
		},
		{
			name:                "unix relative does not exist",
			rawAddress:          "unix:" + unixSocketPath + ".does_not_exist", // "unix:../../tmp/temp-socket.does_not_exist"
			expectDialFailure:   false,
			expectHealthFailure: true,
		},
		{
			// Gitaly does not support connections that do not have a scheme.
			name:              "tcp localhost no prefix",
			rawAddress:        "localhost:" + connectionMap["tcp"], // "localhost:1234"
			expectDialFailure: true,
		},
		{
			name:              "invalid",
			rawAddress:        ".",
			expectDialFailure: true,
		},
		{
			name:              "empty",
			rawAddress:        "",
			expectDialFailure: true,
		},
		{
			name:              "dial fail if there is no listener on address",
			rawAddress:        "tcp://invalid.address",
			dialOpts:          FailOnNonTempDialError(),
			expectDialFailure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if emitProxyWarning() {
				t.Log("WARNING. Proxy configuration detected from environment settings. This test failure may be related to proxy configuration. Please process with caution")
			}

			if tt.envSSLCertFile != "" {
				defer testhelper.ModifyEnvironment(t, gitaly_x509.SSLCertFile, tt.envSSLCertFile)()
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			conn, err := Dial(tt.rawAddress, tt.dialOpts)
			if tt.expectDialFailure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			defer conn.Close()

			_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			if tt.expectHealthFailure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

type testSvc struct {
	proxytestdata.TestServiceServer
	PingMethod       func(context.Context, *proxytestdata.PingRequest) (*proxytestdata.PingResponse, error)
	PingStreamMethod func(stream proxytestdata.TestService_PingStreamServer) error
}

func (ts *testSvc) Ping(ctx context.Context, r *proxytestdata.PingRequest) (*proxytestdata.PingResponse, error) {
	if ts.PingMethod != nil {
		return ts.PingMethod(ctx, r)
	}

	return &proxytestdata.PingResponse{}, nil
}

func (ts *testSvc) PingStream(stream proxytestdata.TestService_PingStreamServer) error {
	if ts.PingStreamMethod != nil {
		return ts.PingStreamMethod(stream)
	}

	return nil
}

func TestDial_Correlation(t *testing.T) {
	t.Run("unary", func(t *testing.T) {
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

		listener, err := net.Listen("unix", serverSocketPath)
		require.NoError(t, err)

		grpcServer := grpc.NewServer(grpc.UnaryInterceptor(grpccorrelation.UnaryServerCorrelationInterceptor()))
		svc := &testSvc{
			PingMethod: func(ctx context.Context, r *proxytestdata.PingRequest) (*proxytestdata.PingResponse, error) {
				cid := correlation.ExtractFromContext(ctx)
				assert.Equal(t, "correlation-id-1", cid)
				return &proxytestdata.PingResponse{}, nil
			},
		}
		proxytestdata.RegisterTestServiceServer(grpcServer, svc)

		go func() { assert.NoError(t, grpcServer.Serve(listener)) }()

		defer grpcServer.Stop()

		ctx, cancel := testhelper.Context()
		defer cancel()

		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		client := proxytestdata.NewTestServiceClient(cc)

		ctx = correlation.ContextWithCorrelation(ctx, "correlation-id-1")
		_, err = client.Ping(ctx, &proxytestdata.PingRequest{})
		require.NoError(t, err)
	})

	t.Run("stream", func(t *testing.T) {
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

		listener, err := net.Listen("unix", serverSocketPath)
		require.NoError(t, err)

		grpcServer := grpc.NewServer(grpc.StreamInterceptor(grpccorrelation.StreamServerCorrelationInterceptor()))
		svc := &testSvc{
			PingStreamMethod: func(stream proxytestdata.TestService_PingStreamServer) error {
				cid := correlation.ExtractFromContext(stream.Context())
				assert.Equal(t, "correlation-id-1", cid)
				_, err := stream.Recv()
				assert.NoError(t, err)
				return stream.Send(&proxytestdata.PingResponse{})
			},
		}
		proxytestdata.RegisterTestServiceServer(grpcServer, svc)

		go func() { assert.NoError(t, grpcServer.Serve(listener)) }()
		defer grpcServer.Stop()

		ctx, cancel := testhelper.Context()
		defer cancel()

		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		client := proxytestdata.NewTestServiceClient(cc)

		ctx = correlation.ContextWithCorrelation(ctx, "correlation-id-1")
		stream, err := client.PingStream(ctx)
		require.NoError(t, err)

		require.NoError(t, stream.Send(&proxytestdata.PingRequest{}))
		require.NoError(t, stream.CloseSend())

		_, err = stream.Recv()
		require.NoError(t, err)
	})
}

func TestDial_Tracing(t *testing.T) {
	t.Run("unary", func(t *testing.T) {
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

		listener, err := net.Listen("unix", serverSocketPath)
		require.NoError(t, err)

		grpcServer := grpc.NewServer(grpc.UnaryInterceptor(grpctracing.UnaryServerTracingInterceptor()))
		svc := &testSvc{
			PingMethod: func(ctx context.Context, r *proxytestdata.PingRequest) (*proxytestdata.PingResponse, error) {
				span, _ := opentracing.StartSpanFromContext(ctx, "health")
				defer span.Finish()
				span.LogKV("was", "called")
				return &proxytestdata.PingResponse{}, nil
			},
		}
		proxytestdata.RegisterTestServiceServer(grpcServer, svc)

		go func() { assert.NoError(t, grpcServer.Serve(listener)) }()
		defer grpcServer.Stop()

		reporter := jaeger.NewInMemoryReporter()
		tracer, closer := jaeger.NewTracer("", jaeger.NewConstSampler(true), reporter)
		defer closer.Close()

		defer func(old opentracing.Tracer) { opentracing.SetGlobalTracer(old) }(opentracing.GlobalTracer())
		opentracing.SetGlobalTracer(tracer)

		span := tracer.StartSpan("unary-check")
		span = span.SetBaggageItem("service", "stub")

		ctx, cancel := testhelper.Context()
		defer cancel()

		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		client := proxytestdata.NewTestServiceClient(cc)

		ctx = opentracing.ContextWithSpan(ctx, span)
		_, err = client.Ping(ctx, &proxytestdata.PingRequest{})
		require.NoError(t, err)

		span.Finish()

		spans := reporter.GetSpans()
		require.Len(t, spans, 3)
		require.Equal(t, "stub", spans[1].BaggageItem("service"))
		require.Equal(t, "stub", spans[2].BaggageItem("service"))
	})

	t.Run("stream", func(t *testing.T) {
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

		listener, err := net.Listen("unix", serverSocketPath)
		require.NoError(t, err)

		grpcServer := grpc.NewServer(grpc.StreamInterceptor(grpctracing.StreamServerTracingInterceptor()))
		svc := &testSvc{
			PingStreamMethod: func(stream proxytestdata.TestService_PingStreamServer) error {
				span, _ := opentracing.StartSpanFromContext(stream.Context(), "health")
				defer span.Finish()
				span.LogKV("was", "called")
				_, err := stream.Recv()
				assert.NoError(t, err)
				return stream.Send(&proxytestdata.PingResponse{})
			},
		}
		proxytestdata.RegisterTestServiceServer(grpcServer, svc)

		go func() { assert.NoError(t, grpcServer.Serve(listener)) }()
		defer grpcServer.Stop()

		reporter := jaeger.NewInMemoryReporter()
		tracer, closer := jaeger.NewTracer("", jaeger.NewConstSampler(true), reporter)
		defer closer.Close()

		defer func(old opentracing.Tracer) { opentracing.SetGlobalTracer(old) }(opentracing.GlobalTracer())
		opentracing.SetGlobalTracer(tracer)

		span := tracer.StartSpan("stream-check")
		span = span.SetBaggageItem("service", "stub")

		ctx, cancel := testhelper.Context()
		defer cancel()

		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		client := proxytestdata.NewTestServiceClient(cc)

		ctx = opentracing.ContextWithSpan(ctx, span)
		stream, err := client.PingStream(ctx)
		require.NoError(t, err)

		require.NoError(t, stream.Send(&proxytestdata.PingRequest{}))
		require.NoError(t, stream.CloseSend())

		_, err = stream.Recv()
		require.NoError(t, err)

		span.Finish()

		spans := reporter.GetSpans()
		require.Len(t, spans, 3)
		require.Equal(t, "stub", spans[0].BaggageItem("service"))
		require.Equal(t, "", spans[1].BaggageItem("service"))
		require.Equal(t, "stub", spans[2].BaggageItem("service"))
	})
}

// healthServer provide a basic GRPC health service endpoint for testing purposes
type healthServer struct {
}

func (*healthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (*healthServer) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "Not implemented")
}

// startTCPListener will start a insecure TCP listener on a random unused port
func startTCPListener() (func(), string, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, "", err
	}
	tcpPort := listener.Addr().(*net.TCPAddr).Port
	address := fmt.Sprintf("%d", tcpPort)

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, address, nil
}

// startUnixListener will start a unix socket listener using a temporary file
func startUnixListener() (func(), string, error) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		return nil, "", err
	}

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, serverSocketPath, nil
}

// startTLSListener will start a secure TLS listener on a random unused port
//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/gitalycert.pem -keyout testdata/gitalykey.pem -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1, DNS:localhost"
func startTLSListener() (func(), string, error) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, "", err
	}
	tcpPort := listener.Addr().(*net.TCPAddr).Port
	address := fmt.Sprintf("%d", tcpPort)

	cert, err := tls.LoadX509KeyPair("testdata/gitalycert.pem", "testdata/gitalykey.pem")
	if err != nil {
		return nil, "", err
	}

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, address, nil
}

var listeners = map[string]func() (func(), string, error){
	"tcp":  startTCPListener,
	"unix": startUnixListener,
	"tls":  startTLSListener,
}

// startListeners will start all the different listeners used in this test
func startListeners() (func(), map[string]string, error) {
	var closers []func()
	connectionMap := map[string]string{}
	for k, v := range listeners {
		closer, address, err := v()
		if err != nil {
			return nil, nil, err
		}
		closers = append(closers, closer)
		connectionMap[k] = address
	}

	return func() {
		for _, v := range closers {
			v()
		}
	}, connectionMap, nil
}

func emitProxyWarning() bool {
	for _, key := range proxyEnvironmentKeys {
		value := os.Getenv(key)
		if value != "" {
			return true
		}
		value = os.Getenv(strings.ToUpper(key))
		if value != "" {
			return true
		}
	}
	return false
}

func TestHealthCheckDialer(t *testing.T) {
	_, addr, cleanup := runServer(t, "token")
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := HealthCheckDialer(DialContext)(ctx, addr, nil)
	require.Equal(t, status.Error(codes.Unauthenticated, "authentication required"), err, "should fail without token configured")

	cc, err := HealthCheckDialer(DialContext)(ctx, addr, []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("token"))})
	require.NoError(t, err)
	cc.Close()
}
