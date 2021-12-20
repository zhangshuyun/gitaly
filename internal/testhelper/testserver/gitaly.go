package testserver

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
	praefectconfig "gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RunGitalyServer starts gitaly server based on the provided cfg and returns a connection address.
// It accepts addition Registrar to register all required service instead of
// calling service.RegisterAll explicitly because it creates a circular dependency
// when the function is used in on of internal/gitaly/service/... packages.
func RunGitalyServer(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) string {
	return StartGitalyServer(t, cfg, rubyServer, registrar, opts...).Address()
}

// StartGitalyServer creates and runs gitaly (and praefect as a proxy) server.
func StartGitalyServer(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) GitalyServer {
	gitalySrv, gitalyAddr, disablePraefect := runGitaly(t, cfg, rubyServer, registrar, opts...)

	if !isPraefectEnabled() || disablePraefect {
		return GitalyServer{
			shutdown: gitalySrv.Stop,
			address:  gitalyAddr,
		}
	}

	praefectServer := runPraefectProxy(t, cfg, gitalyAddr)
	return GitalyServer{
		shutdown: func() {
			praefectServer.Shutdown()
			gitalySrv.Stop()
		},
		address: praefectServer.Address(),
	}
}

func runPraefectProxy(t testing.TB, gitalyCfg config.Cfg, gitalyAddr string) PraefectServer {
	return StartPraefect(t, praefectconfig.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		Auth: auth.Config{
			Token: gitalyCfg.Auth.Token,
		},
		DB: testdb.GetConfig(t, testdb.New(t).Name),
		Failover: praefectconfig.Failover{
			Enabled:          true,
			ElectionStrategy: praefectconfig.ElectionStrategyLocal,
		},
		Replication: praefectconfig.DefaultReplicationConfig(),
		Logging: gitalylog.Config{
			Format: "json",
			Level:  "panic",
		},
		ForceCreateRepositories: true,
		VirtualStorages: []*praefectconfig.VirtualStorage{
			{
				// Only single storage will be served by the Praefect instance. We
				// can't include more as it is invalid to use same address for
				// different storages.
				Name: gitalyCfg.Storages[0].Name,
				Nodes: []*praefectconfig.Node{
					{
						Storage: gitalyCfg.Storages[0].Name,
						Address: gitalyAddr,
						Token:   gitalyCfg.Auth.Token,
					},
				},
			},
		},
	})
}

// GitalyServer is a helper that carries additional info and
// functionality about gitaly (+praefect) server.
type GitalyServer struct {
	shutdown func()
	address  string
}

// Shutdown terminates running gitaly (+praefect) server.
func (gs GitalyServer) Shutdown() {
	gs.shutdown()
}

// Address returns address of the running gitaly (or praefect) service.
func (gs GitalyServer) Address() string {
	return gs.address
}

// waitHealthy waits until the server hosted at address becomes healthy. Times out after a fixed
// amount of time.
func waitHealthy(t testing.TB, addr string, authToken string) {
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	if authToken != "" {
		grpcOpts = append(grpcOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := client.DialContext(ctx, addr, grpcOpts)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	healthClient := healthpb.NewHealthClient(conn)

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "server not yet ready to serve")
}

func runGitaly(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) (*grpc.Server, string, bool) {
	t.Helper()

	var gsd gitalyServerDeps
	for _, opt := range opts {
		gsd = opt(gsd)
	}

	deps := gsd.createDependencies(t, cfg, rubyServer)
	t.Cleanup(func() { gsd.conns.Close() })

	serverFactory := server.NewGitalyServerFactory(
		cfg,
		gsd.logger.WithField("test", t.Name()),
		deps.GetBackchannelRegistry(),
		deps.GetDiskCache(),
		deps.GetLimitHandler(),
	)

	if cfg.InternalSocketDir != "" {
		internalServer, err := serverFactory.CreateInternal()
		require.NoError(t, err)
		t.Cleanup(internalServer.Stop)

		registrar(internalServer, deps)
		registerHealthServerIfNotRegistered(internalServer)

		require.NoError(t, os.MkdirAll(cfg.InternalSocketDir, 0o700))
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(cfg.InternalSocketDir)) })

		internalListener, err := net.Listen("unix", cfg.GitalyInternalSocketPath())
		require.NoError(t, err)
		go func() {
			assert.NoError(t, internalServer.Serve(internalListener), "failure to serve internal gRPC")
		}()

		waitHealthy(t, "unix://"+internalListener.Addr().String(), cfg.Auth.Token)
	}

	externalServer, err := serverFactory.CreateExternal(cfg.TLS.CertPath != "" && cfg.TLS.KeyPath != "")
	require.NoError(t, err)
	t.Cleanup(externalServer.Stop)

	registrar(externalServer, deps)
	registerHealthServerIfNotRegistered(externalServer)

	var listener net.Listener
	var addr string
	switch {
	case cfg.TLSListenAddr != "":
		listener, err = net.Listen("tcp", cfg.TLSListenAddr)
		require.NoError(t, err)
		_, port, err := net.SplitHostPort(listener.Addr().String())
		require.NoError(t, err)
		addr = "tls://localhost:" + port
	case cfg.ListenAddr != "":
		listener, err = net.Listen("tcp", cfg.ListenAddr)
		require.NoError(t, err)
		addr = "tcp://" + listener.Addr().String()
	default:
		serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
		listener, err = net.Listen("unix", serverSocketPath)
		require.NoError(t, err)
		addr = "unix://" + serverSocketPath
	}

	go func() {
		assert.NoError(t, externalServer.Serve(listener), "failure to serve external gRPC")
	}()

	waitHealthy(t, addr, cfg.Auth.Token)

	return externalServer, addr, gsd.disablePraefect
}

func registerHealthServerIfNotRegistered(srv *grpc.Server) {
	if _, found := srv.GetServiceInfo()["grpc.health.v1.Health"]; !found {
		// we should register health service as it is used for the health checks
		// praefect service executes periodically (and on the bootstrap step)
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}
}

type gitalyServerDeps struct {
	disablePraefect  bool
	logger           *logrus.Logger
	conns            *client.Pool
	locator          storage.Locator
	txMgr            transaction.Manager
	hookMgr          hook.Manager
	gitlabClient     gitlab.Client
	gitCmdFactory    git.CommandFactory
	linguist         *linguist.Instance
	backchannelReg   *backchannel.Registry
	catfileCache     catfile.Cache
	diskCache        cache.Cache
	packObjectsCache streamcache.Cache
	limitHandler     *limithandler.LimiterMiddleware
}

func (gsd *gitalyServerDeps) createDependencies(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server) *service.Dependencies {
	if gsd.logger == nil {
		gsd.logger = testhelper.NewDiscardingLogger(t)
	}

	if gsd.conns == nil {
		gsd.conns = client.NewPool()
	}

	if gsd.locator == nil {
		gsd.locator = config.NewLocator(cfg)
	}

	if gsd.gitlabClient == nil {
		gsd.gitlabClient = gitlab.NewMockClient(
			t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
		)
	}

	if gsd.backchannelReg == nil {
		gsd.backchannelReg = backchannel.NewRegistry()
	}

	if gsd.txMgr == nil {
		gsd.txMgr = transaction.NewManager(cfg, gsd.backchannelReg)
	}

	if gsd.gitCmdFactory == nil {
		gsd.gitCmdFactory = git.NewExecCommandFactory(cfg)
	}

	if gsd.hookMgr == nil {
		gsd.hookMgr = hook.NewManager(cfg, gsd.locator, gsd.gitCmdFactory, gsd.txMgr, gsd.gitlabClient)
	}

	if gsd.linguist == nil {
		var err error
		gsd.linguist, err = linguist.New(cfg, gsd.gitCmdFactory)
		require.NoError(t, err)
	}

	if gsd.catfileCache == nil {
		cache := catfile.NewCache(cfg)
		gsd.catfileCache = cache
		t.Cleanup(cache.Stop)
	}

	if gsd.diskCache == nil {
		gsd.diskCache = cache.New(cfg, gsd.locator)
	}

	if gsd.packObjectsCache == nil {
		gsd.packObjectsCache = streamcache.New(cfg.PackObjectsCache, gsd.logger)
		t.Cleanup(gsd.packObjectsCache.Stop)
	}

	if gsd.limitHandler == nil {
		gsd.limitHandler = limithandler.New(cfg, limithandler.LimitConcurrencyByRepo)
	}

	return &service.Dependencies{
		Cfg:                 cfg,
		RubyServer:          rubyServer,
		ClientPool:          gsd.conns,
		StorageLocator:      gsd.locator,
		TransactionManager:  gsd.txMgr,
		GitalyHookManager:   gsd.hookMgr,
		GitCmdFactory:       gsd.gitCmdFactory,
		Linguist:            gsd.linguist,
		BackchannelRegistry: gsd.backchannelReg,
		GitlabClient:        gsd.gitlabClient,
		CatfileCache:        gsd.catfileCache,
		DiskCache:           gsd.diskCache,
		PackObjectsCache:    gsd.packObjectsCache,
		LimitHandler:        gsd.limitHandler,
	}
}

// GitalyServerOpt is a helper type to shorten declarations.
type GitalyServerOpt func(gitalyServerDeps) gitalyServerDeps

// WithLogger sets a logrus.Logger instance that will be used for gitaly services initialisation.
func WithLogger(logger *logrus.Logger) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.logger = logger
		return deps
	}
}

// WithLocator sets a storage.Locator instance that will be used for gitaly services initialisation.
func WithLocator(locator storage.Locator) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.locator = locator
		return deps
	}
}

// WithGitCommandFactory sets a git.CommandFactory instance that will be used for gitaly services
// initialisation.
func WithGitCommandFactory(gitCmdFactory git.CommandFactory) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitCmdFactory = gitCmdFactory
		return deps
	}
}

// WithGitLabClient sets gitlab.Client instance that will be used for gitaly services initialisation.
func WithGitLabClient(gitlabClient gitlab.Client) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitlabClient = gitlabClient
		return deps
	}
}

// WithHookManager sets hook.Manager instance that will be used for gitaly services initialisation.
func WithHookManager(hookMgr hook.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.hookMgr = hookMgr
		return deps
	}
}

// WithTransactionManager sets transaction.Manager instance that will be used for gitaly services initialisation.
func WithTransactionManager(txMgr transaction.Manager) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.txMgr = txMgr
		return deps
	}
}

// WithDisablePraefect disables setup and usage of the praefect as a proxy before the gitaly service.
func WithDisablePraefect() GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.disablePraefect = true
		return deps
	}
}

// WithBackchannelRegistry sets backchannel.Registry instance that will be used for gitaly services initialisation.
func WithBackchannelRegistry(backchannelReg *backchannel.Registry) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.backchannelReg = backchannelReg
		return deps
	}
}

// WithDiskCache sets the cache.Cache instance that will be used for gitaly services initialisation.
func WithDiskCache(diskCache cache.Cache) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.diskCache = diskCache
		return deps
	}
}

func isPraefectEnabled() bool {
	_, ok := os.LookupEnv("GITALY_TEST_WITH_PRAEFECT")
	return ok
}
