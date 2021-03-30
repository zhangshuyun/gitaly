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
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
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

	checkHealth := func(t *testing.T, sf *GitalyServerFactory, schema, addr string) healthpb.HealthClient {
		t.Helper()

		var cc *grpc.ClientConn
		if schema == starter.TLS {
			listener, err := net.Listen(starter.TCP, addr)
			require.NoError(t, err)
			t.Cleanup(func() { listener.Close() })

			srv, err := sf.Create(true)
			require.NoError(t, err)
			healthpb.RegisterHealthServer(srv, health.NewServer())
			go srv.Serve(listener)

			certPool, err := x509.SystemCertPool()
			require.NoError(t, err)

			pem, err := ioutil.ReadFile(sf.cfg.TLS.CertPath)
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
			t.Cleanup(func() { listener.Close() })

			srv, err := sf.Create(false)
			require.NoError(t, err)
			healthpb.RegisterHealthServer(srv, health.NewServer())
			go srv.Serve(listener)

			endpoint, err := starter.ComposeEndpoint(schema, listener.Addr().String())
			require.NoError(t, err)

			cc, err = client.Dial(endpoint, nil)
			require.NoError(t, err)
		}

		t.Cleanup(func() { cc.Close() })

		healthClient := healthpb.NewHealthClient(cc)

		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
		return healthClient
	}

	t.Run("insecure", func(t *testing.T) {
		cfg := testcfg.Build(t)
		sf := NewGitalyServerFactory(cfg, backchannel.NewRegistry())

		checkHealth(t, sf, starter.TCP, "localhost:0")
	})

	t.Run("secure", func(t *testing.T) {
		certFile, keyFile, remove := testhelper.GenerateTestCerts(t)
		t.Cleanup(remove)

		cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{TLS: config.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		}}))

		sf := NewGitalyServerFactory(cfg, backchannel.NewRegistry())
		t.Cleanup(sf.Stop)

		checkHealth(t, sf, starter.TLS, "localhost:0")
	})

	t.Run("all services must be stopped", func(t *testing.T) {
		cfg := testcfg.Build(t)
		sf := NewGitalyServerFactory(cfg, backchannel.NewRegistry())
		t.Cleanup(sf.Stop)

		tcpHealthClient := checkHealth(t, sf, starter.TCP, "localhost:0")

		socket := testhelper.GetTemporaryGitalySocketFileName(t)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(socket)) })

		socketHealthClient := checkHealth(t, sf, starter.Unix, socket)

		sf.GracefulStop() // stops all started servers(listeners)

		_, tcpErr := tcpHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(tcpErr))

		_, socketErr := socketHealthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.Equal(t, codes.Unavailable, status.Code(socketErr))
	})
}
