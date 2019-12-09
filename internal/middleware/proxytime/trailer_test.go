package proxytime_test

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/proxytime"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestTrailerTracker(t *testing.T) {
	srv, socket := runHealthServer(t)
	defer srv.Stop()

	tt := proxytime.NewTrailerTracker()

	id := "abc1234"
	client := newHealthConnection(t, socket)

	ctx, cancel := testhelper.Context()
	defer cancel()

	trailerOption, err := tt.Trailer(id)
	require.NoError(t, err)
	_, err = client.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, trailerOption)
	require.NoError(t, err)

	trailer, err := tt.RemoveTrailer(id)
	require.NoError(t, err)
	require.Len(t, *trailer, 1)
	require.Len(t, trailer.Get("gitaly-time"), 1)
	gitalyTime, err := strconv.ParseFloat(trailer.Get("gitaly-time")[0], 64)
	require.NoError(t, err)

	require.Greater(t, gitalyTime, 0.0)
}

func newHealthConnection(t *testing.T, serverSocketPath string) grpc_health_v1.HealthClient {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return grpc_health_v1.NewHealthClient(conn)
}

func runHealthServer(t *testing.T) (*grpc.Server, string) {
	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(proxytime.StreamGitalyTime),
		grpc.UnaryInterceptor(proxytime.UnaryGitalyTime),
	}

	server := grpc.NewServer(opts...)

	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthSrvr)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}
