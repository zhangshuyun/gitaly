package server

import (
	"net"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	netctx "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestSanity(t *testing.T) {
	srv, serverSocketPath := runServer(t)
	defer srv.Stop()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := dial(serverSocketPath, connOpts)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, healthCheck(conn))
}

func TestAuthFailures(t *testing.T) {
	defer func(oldAuth config.Auth) {
		config.Config.Auth = oldAuth
	}(config.Config.Auth)
	config.Config.Auth.Token = "quxbaz"

	testCases := []struct {
		desc string
		opts []grpc.DialOption
		code codes.Code
	}{
		{desc: "no auth", opts: nil, code: codes.Unauthenticated},
		{
			desc: "invalid auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(brokenAuth{})},
			code: codes.Unauthenticated,
		},
		{
			desc: "wrong secret",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials("foobar"))},
			code: codes.PermissionDenied,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			srv, serverSocketPath := runServer(t)
			defer srv.Stop()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()
			testhelper.RequireGrpcError(t, healthCheck(conn), tc.code)
		})
	}
}

func TestAuthSuccess(t *testing.T) {
	defer func(oldAuth config.Auth) {
		config.Config.Auth = oldAuth
	}(config.Config.Auth)

	testCases := []struct {
		desc     string
		opts     []grpc.DialOption
		required bool
		token    string
	}{
		{desc: "no auth, not required"},
		{
			desc:  "incorrect auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials("incorrect"))},
			token: "foobar",
		},
		{
			desc:  "correct auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials("foobar"))},
			token: "foobar",
		},
		{
			desc:     "correct auth, required",
			opts:     []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials("foobar"))},
			token:    "foobar",
			required: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			config.Config.Auth = config.Auth{
				Transitioning: !tc.required,
				Token:         tc.token,
			}

			srv, serverSocketPath := runServer(t)
			defer srv.Stop()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()
			assert.NoError(t, healthCheck(conn), tc.desc)
		})
	}
}

type brokenAuth struct{}

func (brokenAuth) RequireTransportSecurity() bool { return false }
func (brokenAuth) GetRequestMetadata(netctx.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer blablabla"}, nil
}

func dial(serverSocketPath string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append(opts, grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	}))

	return grpc.Dial(serverSocketPath, opts...)
}

func healthCheck(conn *grpc.ClientConn) error {
	ctx, cancel := testhelper.Context()
	defer cancel()

	client := healthpb.NewHealthClient(conn)
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func runServer(t *testing.T) (*grpc.Server, string) {
	srv := New(nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	go srv.Serve(listener)

	return srv, serverSocketPath
}
