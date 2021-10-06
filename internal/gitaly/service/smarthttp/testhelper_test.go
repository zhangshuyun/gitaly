package smarthttp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

const (
	pktFlushStr = "0000"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func startSmartHTTPServer(t *testing.T, cfg config.Cfg, serverOpts ...ServerOpt) testserver.GitalyServer {
	return testserver.StartGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSmartHTTPServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetDiskCache(),
			serverOpts...,
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
	})
}

func runSmartHTTPServer(t *testing.T, cfg config.Cfg, serverOpts ...ServerOpt) string {
	gitalyServer := startSmartHTTPServer(t, cfg, serverOpts...)
	return gitalyServer.Address()
}

// TODO: remove this method and use runSmartHTTPServer after
// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/1218
func runSmartHTTPServerWithoutPraefect(t *testing.T, cfg config.Cfg, serverOpts ...ServerOpt) string {
	gitalyServer := testserver.StartGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSmartHTTPServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetDiskCache(),
			serverOpts...,
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
	}, testserver.WithDisablePraefect())
	return gitalyServer.Address()
}

func newSmartHTTPClient(t *testing.T, serverSocketPath, token string) (gitalypb.SmartHTTPServiceClient, *grpc.ClientConn) {
	t.Helper()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)

	return gitalypb.NewSmartHTTPServiceClient(conn), conn
}

func newMuxedSmartHTTPClient(t *testing.T, ctx context.Context, serverSocketPath, token string, serverFactory backchannel.ServerFactory) gitalypb.SmartHTTPServiceClient {
	t.Helper()

	conn, err := client.Dial(
		ctx,
		serverSocketPath,
		[]grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
		backchannel.NewClientHandshaker(testhelper.DiscardTestEntry(t), serverFactory),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return gitalypb.NewSmartHTTPServiceClient(conn)
}
