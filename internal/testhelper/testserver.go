package testhelper

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

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
