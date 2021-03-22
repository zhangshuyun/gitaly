package hook

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
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

func setupHookService(t testing.TB) (config.Cfg, *gitalypb.Repository, string, gitalypb.HookServiceClient) {
	t.Helper()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	serverSocketPath := runHooksServer(t, cfg)
	client, conn := newHooksClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	return cfg, repo, repoPath, client
}

func newHooksClient(t testing.TB, serverSocketPath string) (gitalypb.HookServiceClient, *grpc.ClientConn) {
	t.Helper()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewHookServiceClient(conn), conn
}

type serverOption func(*server)

func runHooksServer(t testing.TB, cfg config.Cfg, serverOpts ...serverOption) string {
	return runHooksServerWithAPI(t, gitalyhook.GitlabAPIStub, cfg, serverOpts...)
}

func runHooksServerWithLogger(t *testing.T, cfg config.Cfg, logger *logrus.Logger) string {
	srv := testhelper.NewServerWithLogger(t, logger, nil, nil)
	return runHooksServerWithAPIAndTestServer(t, srv, gitalyhook.GitlabAPIStub, cfg)
}

func runHooksServerWithAPI(t testing.TB, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg, serverOpts ...serverOption) string {
	return runHooksServerWithAPIAndTestServer(t, testhelper.NewServer(t, nil, nil), gitlabAPI, cfg, serverOpts...)
}

func runHooksServerWithAPIAndTestServer(t testing.TB, srv *testhelper.TestServer, gitlabAPI gitalyhook.GitlabAPI, cfg config.Cfg, serverOpts ...serverOption) string {
	t.Helper()

	hookServer := NewServer(
		cfg,
		gitalyhook.NewManager(config.NewLocator(cfg), transaction.NewManager(cfg), gitlabAPI, cfg),
		git.NewExecCommandFactory(cfg),
	)
	for _, opt := range serverOpts {
		opt(hookServer.(*server))
	}

	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hookServer)

	srv.Start(t)
	t.Cleanup(srv.Stop)

	return "unix://" + srv.Socket()
}
