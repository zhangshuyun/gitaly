package cleanup

import (
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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

func setupCleanupService(t *testing.T) (config.Cfg, *gitalypb.Repository, string, gitalypb.CleanupServiceClient) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	serverSocketPath := runCleanupServiceServer(t, cfg)

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	return cfg, repo, repoPath, client
}

func runCleanupServiceServer(t *testing.T, cfg config.Cfg) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
		gitalypb.RegisterCleanupServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
	})
}

func newCleanupServiceClient(t *testing.T, serverSocketPath string) (gitalypb.CleanupServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewCleanupServiceClient(conn), conn
}
