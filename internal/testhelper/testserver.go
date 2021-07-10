package testhelper

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// WriteTemporaryGitalyConfigFile writes a gitaly toml file into a temporary directory. It returns the path to
// the file as well as a cleanup function
func WriteTemporaryGitalyConfigFile(t testing.TB, tempDir, gitlabURL, user, password, secretFile string) (string, func()) {
	path := filepath.Join(tempDir, "config.toml")
	contents := fmt.Sprintf(`
[gitlab]
  url = "%s"
  secret_file = "%s"
  [gitlab.http-settings]
    user = %q
    password = %q
`, gitlabURL, secretFile, user, password)

	require.NoError(t, ioutil.WriteFile(path, []byte(contents), 0644))
	return path, func() {
		require.NoError(t, os.RemoveAll(path))
	}
}

// NewServerWithHealth creates a new gRPC server with the health server set up.
// It will listen on the socket identified by `socketName`.
func NewServerWithHealth(t testing.TB, socketName string) *health.Server {
	lis, err := net.Listen("unix", socketName)
	require.NoError(t, err)

	return NewHealthServerWithListener(t, lis)
}

// NewHealthServerWithListener creates a new gRPC server with the health server
// set up. It will listen on the given listener.
func NewHealthServerWithListener(t testing.TB, listener net.Listener) *health.Server {
	srv := grpc.NewServer()
	healthSrvr := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrvr)

	t.Cleanup(srv.Stop)
	go func() { require.NoError(t, srv.Serve(listener)) }()

	return healthSrvr
}
