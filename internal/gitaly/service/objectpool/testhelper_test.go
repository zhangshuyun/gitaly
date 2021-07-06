package objectpool

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
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

func setup(t *testing.T, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, storage.Locator, gitalypb.ObjectPoolServiceClient) {
	t.Helper()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	locator := config.NewLocator(cfg)
	addr := runObjectPoolServer(t, cfg, locator, testhelper.DiscardTestLogger(t), opts...)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return cfg, repo, repoPath, locator, gitalypb.NewObjectPoolServiceClient(conn)
}

func runObjectPoolServer(t *testing.T, cfg config.Cfg, locator storage.Locator, logger *logrus.Logger, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
		gitalypb.RegisterObjectPoolServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(
			deps.GetCfg(),
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
		))
	}, append(opts, testserver.WithLocator(locator), testserver.WithLogger(logger))...)
}
