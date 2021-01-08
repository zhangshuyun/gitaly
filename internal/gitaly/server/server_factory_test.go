package server

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestGitalyServerFactory(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	checkHealth := func(t *testing.T, sf *GitalyServerFactory, schema, addr string) (healthpb.HealthClient, testhelper.Cleanup) {
		t.Helper()

		var cleanups []testhelper.Cleanup

		var cc *grpc.ClientConn
		if schema == starter.TLS {
			listener, err := net.Listen(starter.TCP, addr)
			require.NoError(t, err)
			cleanups = append(cleanups, func() { listener.Close() })

			go sf.Serve(listener, true)

			certPool, err := x509.SystemCertPool()
			require.NoError(t, err)

			pem, err := ioutil.ReadFile(config.Config.TLS.CertPath)
			require.NoError(t, err)

			require.True(t, certPool.AppendCertsFromPEM(pem))

			creds := credentials.NewTLS(&tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			})

			cc, err = grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(creds))
			require.NoError(t, err)
		} else {
			listener, err := net.Listen(schema, addr)
			require.NoError(t, err)
			cleanups = append(cleanups, func() { listener.Close() })

			go sf.Serve(listener, false)

			endpoint, err := starter.ComposeEndpoint(schema, listener.Addr().String())
			require.NoError(t, err)

			cc, err = client.Dial(endpoint, nil)
			require.NoError(t, err)
		}

		cleanups = append(cleanups, func() { cc.Close() })

		healthClient := healthpb.NewHealthClient(cc)

		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
		return healthClient, func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
		}
	}

	t.Run("insecure", func(t *testing.T) {
		sf := NewGitalyServerFactory(config.Config, nil, nil)

		_, cleanup := checkHealth(t, sf, starter.TCP, "localhost:0")
		defer cleanup()
	})

	t.Run("secure", func(t *testing.T) {
		certFile, keyFile, remove := testhelper.GenerateTestCerts(t)
		defer remove()

		defer func(old config.TLS) { config.Config.TLS = old }(config.Config.TLS)
		config.Config.TLS = config.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		}

		sf := NewGitalyServerFactory(config.Config, nil, nil)
		defer sf.Stop()

		_, cleanup := checkHealth(t, sf, starter.TLS, "localhost:0")
		defer cleanup()
	})

	t.Run("all services must be stopped", func(t *testing.T) {
		sf := NewGitalyServerFactory(config.Config, nil, nil)
		defer sf.Stop()

		tcpHealthClient, tcpCleanup := checkHealth(t, sf, starter.TCP, "localhost:0")
		defer tcpCleanup()

		socket := testhelper.GetTemporaryGitalySocketFileName()
		defer func() { require.NoError(t, os.RemoveAll(socket)) }()

		socketHealthClient, unixCleanup := checkHealth(t, sf, starter.Unix, socket)
		defer unixCleanup()

		sf.GracefulStop() // stops all started servers(listeners)

		_, tcpErr := tcpHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(tcpErr))

		_, socketErr := socketHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(socketErr))
	})
}
