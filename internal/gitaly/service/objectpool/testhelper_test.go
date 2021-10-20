package objectpool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func setup(t *testing.T, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, storage.Locator, gitalypb.ObjectPoolServiceClient) {
	t.Helper()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	testcfg.BuildGitalyHooks(t, cfg)

	locator := config.NewLocator(cfg)
	addr := runObjectPoolServer(t, cfg, locator, testhelper.DiscardTestLogger(t), opts...)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return cfg, repo, repoPath, locator, gitalypb.NewObjectPoolServiceClient(conn)
}

func runObjectPoolServer(t *testing.T, cfg config.Cfg, locator storage.Locator, logger *logrus.Logger, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterObjectPoolServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(
			deps.GetCfg(),
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
	}, append(opts, testserver.WithLocator(locator), testserver.WithLogger(logger))...)
}

// initObjectPool creates a new empty object pool in the given storage.
func initObjectPool(t testing.TB, cfg config.Cfg, storage config.Storage) *objectpool.ObjectPool {
	t.Helper()

	relativePath := gittest.NewObjectPoolName(t)
	gittest.InitRepoDir(t, storage.Path, relativePath)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	pool, err := objectpool.NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		git.NewExecCommandFactory(cfg),
		catfileCache,
		transaction.NewManager(cfg, backchannel.NewRegistry()),
		storage.Name,
		relativePath,
	)
	require.NoError(t, err)

	poolPath := filepath.Join(storage.Path, relativePath)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(poolPath)) })

	return pool
}
