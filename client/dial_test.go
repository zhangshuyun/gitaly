package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var proxyEnvironmentKeys = []string{"http_proxy", "https_proxy", "no_proxy"}

// Note that this test is primarily to ensure that GRPC is working in the way we expect, to avoid issues such as
// https://gitlab.com/gitlab-org/gitaly/issues/1447 and https://gitlab.com/gitlab-org/gitaly/merge_requests/974
// in future. This functionality has also changed between versions of GRPC, so having this test allows us to catch
// problems early on, instead of in staging.
func TestDial(t *testing.T) {
	var proxyWarning string
	if emitProxyWarning() {
		proxyWarning = "WARNING. Proxy configuration detected from environment settings. This test failure may be related to proxy configuration. Please process with caution"
	}

	stop, connectionMap, err := startListeners()
	require.NoError(t, err, "start listeners: %v. %s", err, proxyWarning)
	defer stop()

	tests := []struct {
		name           string
		rawAddress     string
		envSSLCertFile string
		expectFailure  bool
	}{
		{
			name:          "tcp_localhost_with_prefix",
			rawAddress:    "tcp://localhost:" + connectionMap["tcp"], // "tcp://localhost:1234"
			expectFailure: false,
		},
		{
			name:           "tls_localhost",
			rawAddress:     "tls://localhost:" + connectionMap["tls"], // "tls://localhost:1234"
			envSSLCertFile: "./testdata/gitalycert.pem",
			expectFailure:  false,
		},
		{
			name:          "unix_proxy_compatible",
			rawAddress:    "unix://" + connectionMap["unix"], // "unix:///tmp/temp-socket"
			expectFailure: false,
		},
		{
			// Gitaly does not support connections that do not have a scheme.
			name:          "tcp_localhost_no_prefix",
			rawAddress:    "localhost:" + connectionMap["tcp"], // "localhost:1234"
			expectFailure: true,
		},
		{
			name:          "invalid",
			rawAddress:    ".",
			expectFailure: true,
		},
		{
			name:          "empty",
			rawAddress:    "",
			expectFailure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSSLCertFile != "" {
				restore := modifyEnvironment("SSL_CERT_FILE", tt.envSSLCertFile)
				defer restore()
			}

			conn, err := Dial(tt.rawAddress, nil)
			if err != nil {
				if tt.expectFailure {
					return
				}

				require.Failf(t, "unable to dial %s, error = %v. %s", tt.rawAddress, err, proxyWarning)
			}

			client := healthpb.NewHealthClient(conn)
			_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{} /*, grpc.FailFast(true)*/)
			if err != nil {
				if tt.expectFailure {
					return
				}

				require.Failf(t, "unable to connect %s, error = %v. %s", tt.rawAddress, err, proxyWarning)
			}

			if tt.expectFailure {
				require.Failf(t, "expected an error, did not encounter one. %s", proxyWarning)
			}
		})
	}
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
	listener, err := net.Listen("tcp", ":0")
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
func startTLSListener() (func(), string, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, "", err
	}
	tcpPort := listener.Addr().(*net.TCPAddr).Port
	address := fmt.Sprintf("%d", tcpPort)

	cert, err := tls.LoadX509KeyPair("./testdata/gitalycert.pem", "./testdata/gitalykey.pem")
	if err != nil {
		return nil, "", err
	}

	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
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

// modifyEnvironment will change an environment variable and return a func suitable
// for `defer` to change the value back.
func modifyEnvironment(key string, value string) func() {
	oldValue, hasOldValue := os.LookupEnv(key)
	os.Setenv(key, value)
	return func() {
		if hasOldValue {
			os.Setenv(key, oldValue)
		} else {
			os.Unsetenv(key)
		}
	}
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
