package objectpool

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func setup(t *testing.T) (config.Cfg, *gitalypb.Repository, string, storage.Locator, gitalypb.ObjectPoolServiceClient, testhelper.Cleanup) {
	t.Helper()

	var deferrer testhelper.Deferrer
	defer deferrer.Call()

	cfg, repo, repoPath, cleanup := testcfg.BuildWithRepo(t)
	deferrer.Add(cleanup)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	locator := config.NewLocator(cfg)
	server, serverSocketPath := runObjectPoolServer(t, cfg, locator)
	deferrer.Add(server.Stop)

	client, conn := newObjectPoolClient(t, serverSocketPath)
	deferrer.Add(func() { conn.Close() })

	closer := deferrer.Relocate()
	return cfg, repo, repoPath, locator, client, closer.Call
}

func runObjectPoolServer(t *testing.T, cfg config.Cfg, locator storage.Locator) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	internalListener, err := net.Listen("unix", cfg.GitalyInternalSocketPath())
	require.NoError(t, err)

	txManager := transaction.NewManager(cfg)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)

	gitalypb.RegisterObjectPoolServiceServer(server, NewServer(cfg, locator, gitCmdFactory))
	gitalypb.RegisterHookServiceServer(server, hookservice.NewServer(cfg, hookManager, gitCmdFactory))

	go server.Serve(listener)
	go server.Serve(internalListener)

	return server, serverSocketPath
}

func newObjectPoolClient(t *testing.T, serverSocketPath string) (gitalypb.ObjectPoolServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (conn net.Conn, err error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "unix", addr)
		}),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewObjectPoolServiceClient(conn), conn
}
