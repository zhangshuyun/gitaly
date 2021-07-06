package teststream

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	gitalyServer "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestTestStreamInsecure(t *testing.T) {
	addr, _ := runServer(t, "tcp", "")
	testPingPong(t, streamrpc.DialNet("tcp", addr), 1024*1024)
}

func TestTestStreamInsecureWithAuth(t *testing.T) {
	addr, _ := runServer(t, "tcp", "supersecret")
	testPingPong(
		t, streamrpc.DialNet("tcp", addr), 1024*1024,
		streamrpc.WithCredentials(gitalyauth.RPCCredentialsV2("supersecret")),
	)
}

func TestTestStreamTLS(t *testing.T) {
	addr, sfTLS := runServer(t, "TLS", "")
	conf := generateClientTLSConfig(t, sfTLS)
	testPingPong(t, streamrpc.DialTLS("tcp", addr, &conf), 1024*1024)
}

func TestTestStreamTLSWithAuth(t *testing.T) {
	addr, sfTLS := runServer(t, "TLS", "supersecret2")
	conf := generateClientTLSConfig(t, sfTLS)
	testPingPong(
		t, streamrpc.DialTLS("tcp", addr, &conf), 1024*1024,
		streamrpc.WithCredentials(gitalyauth.RPCCredentialsV2("supersecret2")),
	)
}

func TestTestStreamUnixSockets(t *testing.T) {
	addr, _ := runServer(t, "unix", "")
	testPingPong(t, streamrpc.DialNet("unix", addr), 1024*1024)
}

func TestTestStreamUnixSocketsWithAuth(t *testing.T) {
	addr, _ := runServer(t, "unix", "supersecret3")
	testPingPong(
		t, streamrpc.DialNet("unix", addr), 1024*1024,
		streamrpc.WithCredentials(gitalyauth.RPCCredentialsV2("supersecret3")),
	)
}

func testPingPong(t *testing.T, dial streamrpc.DialFunc, size int64, opts ...streamrpc.CallOption) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	in := make([]byte, size)
	_, err := rand.Read(in)
	require.NoError(t, err)

	var out []byte
	require.NotEqual(t, in, out)
	require.NoError(t, streamrpc.Call(
		ctx,
		dial,
		"/gitaly.TestStreamService/TestStream",
		&gitalypb.TestStreamRequest{Size: size},
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

			return c.Close()
		},
		opts...,
	))

	require.Equal(t, in, out, "byte stream works")
}

func runServer(t *testing.T, connectionType string, authToken string) (string, *config.TLS) {
	t.Helper()
	testhelper.Configure()

	cfg := generateServerConfig(t, connectionType, authToken)

	sf := createServerFactory(t, cfg)
	addr := "localhost:0"
	schema := "tcp"
	secure := false
	if connectionType == "unix" {
		addr = testhelper.GetTemporaryGitalySocketFileName(t)
		schema = "unix"
	}
	if connectionType == "TLS" {
		secure = true
	}

	listener, err := net.Listen(schema, addr)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	srv, err := sf.CreateExternal(secure)
	require.NoError(t, err)

	go srv.Serve(listener)

	return listener.Addr().String(), &cfg.TLS
}

func createServerFactory(t *testing.T, cfg config.Cfg) *gitalyServer.GitalyServerFactory {
	registry := backchannel.NewRegistry()
	cache := cache.New(cfg, config.NewLocator(cfg))
	streamRPCServer := streamrpc.NewServer()
	gitalypb.RegisterTestStreamServiceServer(streamRPCServer, NewServer())
	return gitalyServer.NewGitalyServerFactory(cfg, testhelper.DiscardTestEntry(t), registry, cache, streamRPCServer)
}

func generateServerConfig(t *testing.T, connectionType string, authToken string) config.Cfg {
	cfg := config.Cfg{}

	if connectionType == "TLS" {
		certFile, keyFile := testhelper.GenerateCerts(t)
		cfg.TLS = config.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		}
	}

	if authToken != "" {
		cfg.Auth = auth.Config{
			Token: authToken,
		}
	}

	return testcfg.Build(t, testcfg.WithBase(cfg))
}

func generateClientTLSConfig(t *testing.T, sfTLS *config.TLS) tls.Config {
	certPool, err := x509.SystemCertPool()
	require.NoError(t, err)

	pem := testhelper.MustReadFile(t, sfTLS.CertPath)
	require.True(t, certPool.AppendCertsFromPEM(pem))

	return tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
		ServerName: "localhost",
	}
}
