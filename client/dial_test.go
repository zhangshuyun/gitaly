package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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

	stop, connectionMap := startListeners(t)
	defer stop()

	unixSocketAbsPath := connectionMap["unix"]

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	unixSocketPath := filepath.Join(tempDir, "gitaly.socket")
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
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

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
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

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
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	clientSendClosed := make(chan struct{})

	// This is our test service. All it does is to create additional spans
	// which should in the end be visible when collecting all registered
	// spans.
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpctracing.UnaryServerTracingInterceptor()),
		grpc.StreamInterceptor(grpctracing.StreamServerTracingInterceptor()),
	)
	svc := &testSvc{
		PingMethod: func(ctx context.Context, r *proxytestdata.PingRequest) (*proxytestdata.PingResponse, error) {
			span, _ := opentracing.StartSpanFromContext(ctx, "nested-span")
			defer span.Finish()
			span.LogKV("was", "called")
			return &proxytestdata.PingResponse{}, nil
		},
		PingStreamMethod: func(stream proxytestdata.TestService_PingStreamServer) error {
			// synchronize the client has returned from CloseSend as the client span finishing
			// races with sending the stream close to the server
			select {
			case <-clientSendClosed:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}

			span, _ := opentracing.StartSpanFromContext(stream.Context(), "nested-span")
			defer span.Finish()
			span.LogKV("was", "called")
			return nil
		},
	}
	proxytestdata.RegisterTestServiceServer(grpcServer, svc)

	go func() { require.NoError(t, grpcServer.Serve(listener)) }()
	defer grpcServer.Stop()

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("unary", func(t *testing.T) {
		reporter := jaeger.NewInMemoryReporter()
		tracer, tracerCloser := jaeger.NewTracer("", jaeger.NewConstSampler(true), reporter)

		defer func(old opentracing.Tracer) { opentracing.SetGlobalTracer(old) }(opentracing.GlobalTracer())
		opentracing.SetGlobalTracer(tracer)

		// This needs to be run after setting up the global tracer as it will cause us to
		// create the span when executing the RPC call further down below.
		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		// We set up a "main" span here, which is going to be what the
		// other spans inherit from. In order to check whether baggage
		// works correctly, we also set up a "stub" baggage item which
		// should be inherited to child contexts.
		span := tracer.StartSpan("unary-check")
		span = span.SetBaggageItem("service", "stub")
		ctx := opentracing.ContextWithSpan(ctx, span)

		// We're now invoking the unary RPC with the span injected into
		// the context. This should create a span that's nested into
		// the "stream-check" span.
		_, err = proxytestdata.NewTestServiceClient(cc).Ping(ctx, &proxytestdata.PingRequest{})
		require.NoError(t, err)

		span.Finish()
		tracerCloser.Close()

		spans := reporter.GetSpans()
		require.Len(t, spans, 3)

		for i, expectedSpan := range []struct {
			baggage   string
			operation string
		}{
			// This is the first span we expect, which is the
			// "health" span which we've manually created inside of
			// PingMethod.
			{baggage: "", operation: "nested-span"},
			// This span is the RPC call to TestService/Ping. It
			// inherits the "unary-check" we set up and thus has
			// baggage.
			{baggage: "stub", operation: "/mwitkow.testproto.TestService/Ping"},
			// And this finally is the outermost span which we
			// manually set up before the RPC call.
			{baggage: "stub", operation: "unary-check"},
		} {
			assert.IsType(t, spans[i], &jaeger.Span{})
			span := spans[i].(*jaeger.Span)

			assert.Equal(t, expectedSpan.baggage, span.BaggageItem("service"), "wrong baggage item for span %d", i)
			assert.Equal(t, expectedSpan.operation, span.OperationName(), "wrong operation name for span %d", i)
		}
	})

	t.Run("stream", func(t *testing.T) {
		reporter := jaeger.NewInMemoryReporter()
		tracer, tracerCloser := jaeger.NewTracer("", jaeger.NewConstSampler(true), reporter)

		defer func(old opentracing.Tracer) { opentracing.SetGlobalTracer(old) }(opentracing.GlobalTracer())
		opentracing.SetGlobalTracer(tracer)

		// This needs to be run after setting up the global tracer as it will cause us to
		// create the span when executing the RPC call further down below.
		cc, err := DialContext(ctx, "unix://"+serverSocketPath, nil)
		require.NoError(t, err)
		defer cc.Close()

		// We set up a "main" span here, which is going to be what the other spans inherit
		// from. In order to check whether baggage works correctly, we also set up a "stub"
		// baggage item which should be inherited to child contexts.
		span := tracer.StartSpan("stream-check")
		span = span.SetBaggageItem("service", "stub")
		ctx := opentracing.ContextWithSpan(ctx, span)

		// We're now invoking the streaming RPC with the span injected into the context.
		// This should create a span that's nested into the "stream-check" span.
		stream, err := proxytestdata.NewTestServiceClient(cc).PingStream(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.CloseSend())
		close(clientSendClosed)

		// wait for the server to finish its spans and close the stream
		resp, err := stream.Recv()
		require.Equal(t, err, io.EOF)
		require.Nil(t, resp)

		span.Finish()
		tracerCloser.Close()

		spans := reporter.GetSpans()
		require.Len(t, spans, 3)

		for i, expectedSpan := range []struct {
			baggage   string
			operation string
		}{
			// This span is the RPC call to TestService/Ping.
			{baggage: "stub", operation: "/mwitkow.testproto.TestService/PingStream"},
			// This is the second span we expect, which is the "nested-span" span which
			// we've manually created inside of PingMethod. This is different than for
			// unary RPCs: given that one can send multiple messages to the RPC, we may
			// see multiple such "nested-span"s being created. And the PingStream span
			// will only be finalized last.
			{baggage: "", operation: "nested-span"},
			// And this finally is the outermost span which we
			// manually set up before the RPC call.
			{baggage: "stub", operation: "stream-check"},
		} {
			if !assert.IsType(t, spans[i], &jaeger.Span{}) {
				continue
			}

			span := spans[i].(*jaeger.Span)
			assert.Equal(t, expectedSpan.baggage, span.BaggageItem("service"), "wrong baggage item for span %d", i)
			assert.Equal(t, expectedSpan.operation, span.OperationName(), "wrong operation name for span %d", i)
		}
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
func startTCPListener(t testing.TB) (func(), string) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	tcpPort := listener.Addr().(*net.TCPAddr).Port
	address := fmt.Sprintf("%d", tcpPort)

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, address
}

// startUnixListener will start a unix socket listener using a temporary file
func startUnixListener(t testing.TB) (func(), string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, serverSocketPath
}

// startTLSListener will start a secure TLS listener on a random unused port
//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/gitalycert.pem -keyout testdata/gitalykey.pem -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1, DNS:localhost"
func startTLSListener(t testing.TB) (func(), string) {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	tcpPort := listener.Addr().(*net.TCPAddr).Port
	address := fmt.Sprintf("%d", tcpPort)

	cert, err := tls.LoadX509KeyPair("testdata/gitalycert.pem", "testdata/gitalykey.pem")
	require.NoError(t, err)

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})))
	healthpb.RegisterHealthServer(grpcServer, &healthServer{})
	go grpcServer.Serve(listener)

	return func() {
		grpcServer.Stop()
	}, address
}

var listeners = map[string]func(testing.TB) (func(), string){
	"tcp":  startTCPListener,
	"unix": startUnixListener,
	"tls":  startTLSListener,
}

// startListeners will start all the different listeners used in this test
func startListeners(t testing.TB) (func(), map[string]string) {
	var closers []func()
	connectionMap := map[string]string{}
	for k, v := range listeners {
		closer, address := v(t)
		closers = append(closers, closer)
		connectionMap[k] = address
	}

	return func() {
		for _, v := range closers {
			v()
		}
	}, connectionMap
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
