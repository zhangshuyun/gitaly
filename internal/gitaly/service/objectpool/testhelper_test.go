package objectpool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
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

// clientWithConn allows for passing through the ClientConn to tests which need
// to access other services than ObjectPoolService.
type clientWithConn struct {
	gitalypb.ObjectPoolServiceClient
	conn *grpc.ClientConn
}

// extractConn returns the underlying ClientConn from the client.
func extractConn(client gitalypb.ObjectPoolServiceClient) *grpc.ClientConn {
	return client.(clientWithConn).conn
}

func setup(ctx context.Context, t *testing.T, opts ...testserver.GitalyServerOpt) (config.Cfg, *gitalypb.Repository, string, storage.Locator, gitalypb.ObjectPoolServiceClient) {
	t.Helper()

	cfg := testcfg.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)

	locator := config.NewLocator(cfg)
	cfg.SocketPath = runObjectPoolServer(t, cfg, locator, testhelper.NewDiscardingLogger(t), opts...)

	conn, err := grpc.Dial(cfg.SocketPath, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	return cfg, repo, repoPath, locator, clientWithConn{ObjectPoolServiceClient: gitalypb.NewObjectPoolServiceClient(conn), conn: conn}
}

func runObjectPoolServer(t *testing.T, cfg config.Cfg, locator storage.Locator, logger *logrus.Logger, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterObjectPoolServiceServer(srv, NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
			deps.GetHousekeepingManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			cfg,
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetHousekeepingManager(),
		))
	}, append(opts, testserver.WithLocator(locator), testserver.WithLogger(logger))...)
}

func newObjectPool(t testing.TB, cfg config.Cfg, storage, relativePath string) *objectpool.ObjectPool {
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	pool, err := objectpool.NewObjectPool(
		config.NewLocator(cfg),
		gittest.NewCommandFactory(t, cfg),
		catfileCache,
		txManager,
		housekeeping.NewManager(txManager),
		storage,
		relativePath,
	)
	require.NoError(t, err)

	return pool
}

// initObjectPool creates a new empty object pool in the given storage.
func initObjectPool(t testing.TB, cfg config.Cfg, storage config.Storage) *objectpool.ObjectPool {
	t.Helper()

	relativePath := gittest.NewObjectPoolName(t)
	gittest.InitRepoDir(t, storage.Path, relativePath)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	pool := newObjectPool(t, cfg, storage.Name, relativePath)

	poolPath := filepath.Join(storage.Path, relativePath)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(poolPath)) })

	return pool
}

// rewrittenObjectPool returns a pool that is rewritten as if it was passed through Praefect. This should be used
// to access the pool on the disk if the tests are running with Praefect in front of them.
func rewrittenObjectPool(ctx context.Context, t testing.TB, cfg config.Cfg, pool *objectpool.ObjectPool) *objectpool.ObjectPool {
	replicaPath := gittest.GetReplicaPath(ctx, t, cfg, pool)
	return newObjectPool(t, cfg, pool.GetStorageName(), replicaPath)
}
