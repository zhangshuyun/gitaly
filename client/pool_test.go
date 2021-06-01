package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	gitaly_auth "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestPoolDial(t *testing.T) {
	_, insecure, cleanup := runServer(t, "")
	defer cleanup()

	creds := "my-little-secret"
	_, secure, cleanup := runServer(t, creds)
	defer cleanup()

	var dialFuncInvocationCounter int

	testCases := []struct {
		desc        string
		poolOptions []PoolOption
		test        func(t *testing.T, ctx context.Context, pool *Pool)
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
		{
			desc: "dialing with dial options succeeds",
			poolOptions: []PoolOption{
				WithDialOptions(grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(creds))),
			},
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				conn, err := pool.Dial(ctx, secure, "") // no creds here
				require.NoError(t, err)
				verifyConnection(t, conn, codes.OK) // auth passes
			},
		},
		{
			desc: "dial options function is invoked per dial",
			poolOptions: []PoolOption{
				WithDialer(func(ctx context.Context, address string, dialOptions []grpc.DialOption) (*grpc.ClientConn, error) {
					dialFuncInvocationCounter++
					return DialContext(ctx, address, dialOptions)
				}),
			},
			test: func(t *testing.T, ctx context.Context, pool *Pool) {
				_, err := pool.Dial(ctx, secure, "")
				require.NoError(t, err)
				assert.Equal(t, 1, dialFuncInvocationCounter)
				_, err = pool.Dial(ctx, insecure, "")
				require.NoError(t, err)
				assert.Equal(t, 2, dialFuncInvocationCounter)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pool := NewPoolWithOptions(tc.poolOptions...)
			defer func() {
				require.NoError(t, pool.Close())
			}()

			ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
			defer cancel()

			tc.test(t, ctx, pool)
		})
	}
}

func runServer(t *testing.T, creds string) (*health.Server, string, func()) {
	return runServerWithAddr(t, creds, "127.0.0.1:0")
}

func runServerWithAddr(t *testing.T, creds, addr string) (*health.Server, string, func()) {
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

	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	errQ := make(chan error)
	go func() {
		errQ <- server.Serve(listener)
	}()

	return healthServer, "tcp://" + listener.Addr().String(), func() {
		server.Stop()
		require.NoError(t, <-errQ)
	}
}

func verifyConnection(t *testing.T, conn *grpc.ClientConn, expectedCode codes.Code) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})

	if expectedCode == codes.OK {
		require.NoError(t, err)
	} else {
		require.Equal(t, expectedCode, status.Code(err))
	}
}

func TestPool_Dial_same_addr_another_token(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, addr, stop1 := runServer(t, "")
	defer func() { stop1() }()

	pool := NewPool()
	defer pool.Close()

	// all good - server is running and serving requests
	conn, err := pool.Dial(ctx, addr, "")
	require.NoError(t, err)
	verifyConnection(t, conn, codes.OK)

	stop1() // stop the server and all open connections
	stop1 = func() {}

	cfg, err := starter.ParseEndpoint(addr)
	require.NoError(t, err)

	// start server on the same address (simulation of service restart) but with token verification enabled
	_, _, stop2 := runServerWithAddr(t, "token", cfg.Addr)
	defer stop2()

	// all good - another server with token verification is running on the same address and new connection was established
	conn, err = pool.Dial(ctx, addr, "token")
	require.NoError(t, err)
	verifyConnection(t, conn, codes.OK)
}
