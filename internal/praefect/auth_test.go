package praefect

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestAuthFailures(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc string
		opts []grpc.DialOption
		code codes.Code
	}{
		{
			desc: "no auth",
			opts: nil,
			code: codes.Unauthenticated,
		},
		{
			desc: "invalid auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(brokenAuth{})},
			code: codes.Unauthenticated,
		},
		{
			desc: "wrong secret new auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("foobar"))},
			code: codes.PermissionDenied,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			srv, serverSocketPath, cleanup := runServer(t, "quxbaz", true)
			defer srv.Stop()
			defer cleanup()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()

			cli := mock.NewSimpleServiceClient(conn)

			_, err = cli.RepoAccessorUnary(ctx, &mock.RepoRequest{})
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestAuthSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	token := "foobar"

	testCases := []struct {
		desc     string
		opts     []grpc.DialOption
		required bool
		token    string
	}{
		{desc: "no auth, not required"},
		{
			desc:  "v2 correct auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token: token,
		},
		{
			desc:  "v2 incorrect auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("incorrect"))},
			token: token,
		},
		{
			desc:     "v2 correct auth, required",
			opts:     []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token:    token,
			required: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			srv, serverSocketPath, cleanup := runServer(t, tc.token, tc.required)
			defer srv.Stop()
			defer cleanup()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()

			cli := gitalypb.NewServerServiceClient(conn)

			_, err = cli.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

			assert.NoError(t, err, tc.desc)
		})
	}
}

type brokenAuth struct{}

func (brokenAuth) RequireTransportSecurity() bool { return false }
func (brokenAuth) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer blablabla"}, nil
}

func dial(serverSocketPath string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(serverSocketPath, opts...)
}

func runServer(t *testing.T, token string, required bool) (*grpc.Server, string, func()) {
	backendToken := "abcxyz"
	backend, cleanup := newMockDownstream(t, backendToken, &mockSvc{})

	conf := config.Config{
		Auth: auth.Config{Token: token, Transitioning: !required},
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-0",
						Address: backend,
						Token:   backendToken,
					},
				},
			},
		},
	}
	logEntry := testhelper.DiscardTestEntry(t)
	queue := datastore.NewPostgresReplicationEventQueue(glsql.NewDB(t))

	nodeMgr, err := nodes.NewManager(logEntry, conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)

	txMgr := transactions.NewManager(conf)

	registry, err := protoregistry.NewFromPaths("praefect/mock/mock.proto")
	require.NoError(t, err)

	coordinator := NewCoordinator(queue, nil, NewNodeManagerRouter(nodeMgr, nil), txMgr, conf, registry)

	srv := NewGRPCServer(conf, logEntry, registry, coordinator.StreamDirector, nodeMgr, txMgr, queue, nil, nil, nil, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	go srv.Serve(listener)

	return srv, "unix://" + serverSocketPath, cleanup
}
