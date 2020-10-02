package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/require"
	gitaly_auth "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestPoolDial(t *testing.T) {
	insecure, cleanup := runServer(t, "")
	defer cleanup()

	creds := "my-little-secret"
	secure, cleanup := runServer(t, creds)
	defer cleanup()

	testCases := []struct {
		desc string
		test func(t *testing.T, ctx context.Context, pool *Pool)
	}{
		{
			desc: "dialing once succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, conn, codes.OK)
			},
		},
		{
			desc: "dialing multiple times succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				for i := 0; i < 10; i++ {
					conn, err := pool.Dial(ctx, insecure, "")
					require.NoError(t, err)
					verifyConnection(t, conn, codes.OK)
				}
			},
		},
		{
			desc: "redialing after close succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, conn, codes.OK)

				require.NoError(t, pool.Close())

				conn, err = pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				verifyConnection(t, conn, codes.OK)
			},
		},
		{
			desc: "dialing invalid fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, "foo/bar", "")
				require.Error(t, err)
				require.Nil(t, conn)
			},
		},
		{
			desc: "dialing empty fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, "", "")
				require.Error(t, err)
				require.Nil(t, conn)
			},
		},
		{
			desc: "dialing concurrently succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				wg := sync.WaitGroup{}

				for i := 0; i < 10; i++ {
					wg.Add(1)

					go func() {
						defer wg.Done()
						conn, err := pool.Dial(ctx, insecure, "")
						require.NoError(t, err)
						verifyConnection(t, conn, codes.OK)
					}()
				}

				wg.Wait()
			},
		},
		{
			desc: "dialing with credentials succeeds",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, creds)
				require.NoError(t, err)
				verifyConnection(t, conn, codes.OK)
			},
		},
		{
			desc: "dialing with invalid credentials fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "invalid-credential")
				require.NoError(t, err)
				verifyConnection(t, conn, codes.PermissionDenied)
			},
		},
		{
			desc: "dialing with missing credentials fails",
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "")
				require.NoError(t, err)
				verifyConnection(t, conn, codes.Unauthenticated)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pool := NewPool()
			defer func() {
				require.NoError(t, pool.Close())
			}()

			ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
			defer cancel()

			tc.test(t, ctx, pool)
		})
	}
}

func runServer(t *testing.T, creds string) (string, func()) {
	t.Helper()

	var opts []grpc.ServerOption
	if creds != "" {
		opts = []grpc.ServerOption{
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				auth.StreamServerInterceptor(gitaly_auth.Config{
					Token: creds,
				}),
			)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				auth.UnaryServerInterceptor(gitaly_auth.Config{
					Token: creds,
				}),
			)),
		}
	}

	server := grpc.NewServer(opts...)

	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("TestService", grpc_health_v1.HealthCheckResponse_SERVING)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errQ := make(chan error)
	go func() {
		errQ <- server.Serve(listener)
	}()

	return "tcp://" + listener.Addr().String(), func() {
		server.Stop()
		require.NoError(t, <-errQ)
	}
}

func verifyConnection(t *testing.T, conn *grpc.ClientConn, expectedCode codes.Code) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: "TestService",
	})

	if expectedCode == codes.OK {
		require.NoError(t, err)
	} else {
		require.Equal(t, expectedCode, status.Code(err))
	}
}
