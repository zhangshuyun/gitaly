package main

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// svcRegistrar is a function that registers a gRPC service with a server
// instance
type svcRegistrar func(*grpc.Server)

func registerHealthService(srv *grpc.Server) {
	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthSrvr)
	healthSrvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
}

func registerServerService(impl gitalypb.ServerServiceServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterServerServiceServer(srv, impl)
	}
}

func listenAndServe(t testing.TB, svcs []svcRegistrar) (net.Listener, testhelper.Cleanup) {
	t.Helper()

	tmp := testhelper.TempDir(t)

	ln, err := net.Listen("unix", filepath.Join(tmp, "gitaly"))
	require.NoError(t, err)

	srv := grpc.NewServer()

	for _, s := range svcs {
		s(srv)
	}

	go func() { require.NoError(t, srv.Serve(ln)) }()

	ctx, cancel := testhelper.Context()
	defer cancel()

	// verify the service is up
	addr := fmt.Sprintf("%s://%s", ln.Addr().Network(), ln.Addr())
	cc, err := grpc.DialContext(ctx, addr, grpc.WithBlock(), grpc.WithInsecure())
	require.NoError(t, err)
	require.NoError(t, cc.Close())

	return ln, func() {
		srv.Stop()
	}
}
