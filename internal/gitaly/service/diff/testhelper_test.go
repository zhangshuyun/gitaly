package diff

import (
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
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

func setupDiffService(t testing.TB) (config.Cfg, *gitalypb.Repository, string, gitalypb.DiffServiceClient) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	cfg.SocketPath = runDiffServer(t, cfg)
	client, conn := newDiffClient(t, cfg.SocketPath)
	t.Cleanup(func() { conn.Close() })

	return cfg, repo, repoPath, client
}

func runDiffServer(t testing.TB, cfg config.Cfg) string {
	t.Helper()

	server := testhelper.NewTestGrpcServer(t, nil, nil)
	t.Cleanup(server.Stop)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	gitalypb.RegisterDiffServiceServer(server, NewServer(cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg)))

	go server.Serve(listener)

	return "unix://" + serverSocketPath
}

func newDiffClient(t testing.TB, serverSocketPath string) (gitalypb.DiffServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)

	return gitalypb.NewDiffServiceClient(conn), conn
}
