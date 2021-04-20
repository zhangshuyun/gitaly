package testserver

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	praefectconfig "gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RunGitalyServer starts gitaly server based on the provided cfg and returns a connection address.
// It accepts addition Registrar to register all required service instead of
// calling service.RegisterAll explicitly because it creates a circular dependency
// when the function is used in on of internal/gitaly/service/... packages.
func RunGitalyServer(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) string {
	_, gitalyAddr, disablePraefect := runGitaly(t, cfg, rubyServer, registrar, opts...)

	praefectBinPath, ok := os.LookupEnv("GITALY_TEST_PRAEFECT_BIN")
	if !ok || disablePraefect {
		return gitalyAddr
	}

	praefectAddr, _ := runPraefectProxy(t, cfg, gitalyAddr, praefectBinPath)
	return praefectAddr
}

func runPraefectProxy(t testing.TB, cfg config.Cfg, gitalyAddr, praefectBinPath string) (string, func()) {
	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	praefectServerSocketPath := "unix://" + testhelper.GetTemporaryGitalySocketFileName(t)

	conf := praefectconfig.Config{
		SocketPath: praefectServerSocketPath,
		Auth: auth.Config{
			Token: cfg.Auth.Token,
		},
		MemoryQueueEnabled: true,
		Failover: praefectconfig.Failover{
			Enabled:           true,
			ElectionStrategy:  praefectconfig.ElectionStrategyLocal,
			BootstrapInterval: config.Duration(time.Microsecond),
			MonitorInterval:   config.Duration(time.Second),
		},
		Replication: praefectconfig.DefaultReplicationConfig(),
		Logging: gitalylog.Config{
			Format: "json",
			Level:  "panic",
		},
	}

	// Only single storage will be served by the praefect instance.
	// We can't include more as it is invalid to use same address for
	// different storages.
	conf.VirtualStorages = []*praefectconfig.VirtualStorage{{
		Name: cfg.Storages[0].Name,
		Nodes: []*praefectconfig.Node{{
			Storage: cfg.Storages[0].Name,
			Address: gitalyAddr,
			Token:   cfg.Auth.Token,
		}},
	}}

	configFilePath := filepath.Join(tempDir, "config.toml")
	configFile, err := os.Create(configFilePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, configFile)

	require.NoError(t, toml.NewEncoder(configFile).Encode(&conf))
	require.NoError(t, configFile.Sync())

	cmd := exec.Command(praefectBinPath, "-config", configFilePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	require.NoError(t, cmd.Start())

	grpcOpts := []grpc.DialOption{grpc.WithInsecure()}
	if cfg.Auth.Token != "" {
		grpcOpts = append(grpcOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(cfg.Auth.Token)))
	}

	conn, err := grpc.Dial(praefectServerSocketPath, grpcOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	waitHealthy(t, conn, 3, time.Second)

	t.Cleanup(func() { _ = cmd.Wait() })
	shutdown := func() { _ = cmd.Process.Kill() }
	t.Cleanup(shutdown)
	return praefectServerSocketPath, shutdown
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

// StartGitalyServer creates and runs gitaly (and praefect as a proxy) server.
func StartGitalyServer(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) GitalyServer {
	gitalySrv, gitalyAddr, disablePraefect := runGitaly(t, cfg, rubyServer, registrar, opts...)

	praefectBinPath, ok := os.LookupEnv("GITALY_TEST_PRAEFECT_BIN")
	if !ok || disablePraefect {
		return GitalyServer{
			shutdown: gitalySrv.Stop,
			address:  gitalyAddr,
		}
	}

	praefectAddr, shutdownPraefect := runPraefectProxy(t, cfg, gitalyAddr, praefectBinPath)
	return GitalyServer{
		shutdown: func() {
			shutdownPraefect()
			gitalySrv.Stop()
		},
		address: praefectAddr,
	}
}

// waitHealthy executes health check request `retries` times and awaits each `timeout` period to respond.
// After `retries` unsuccessful attempts it returns an error.
// Returns immediately without an error once get a successful health check response.
func waitHealthy(t testing.TB, conn *grpc.ClientConn, retries int, timeout time.Duration) {
	for i := 0; i < retries; i++ {
		if IsHealthy(conn, timeout) {
			return
		}
	}

	require.FailNow(t, "server not yet ready to serve")
}

// IsHealthy creates a health client to passed in connection and send `Check` request.
// It waits for `timeout` duration to get response back.
// It returns `true` only if remote responds with `SERVING` status.
func IsHealthy(conn *grpc.ClientConn, timeout time.Duration) bool {
	healthClient := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return false
	}

	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return false
	}

	return true
}

func runGitaly(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server, registrar func(srv *grpc.Server, deps *service.Dependencies), opts ...GitalyServerOpt) (*grpc.Server, string, bool) {
	t.Helper()

	var gsd gitalyServerDeps
	for _, opt := range opts {
		gsd = opt(gsd)
	}

	deps := gsd.createDependencies(t, cfg, rubyServer)
	t.Cleanup(func() { gsd.conns.Close() })

	srv, err := server.New(cfg.TLS.CertPath != "" && cfg.TLS.KeyPath != "", cfg, gsd.logger.WithField("test", t.Name()), deps.GetBackchannelRegistry())
	require.NoError(t, err)
	t.Cleanup(srv.Stop)

	registrar(srv, deps)
	if _, found := srv.GetServiceInfo()["grpc.health.v1.Health"]; !found {
		// we should register health service as it is used for the health checks
		// praefect service executes periodically (and on the bootstrap step)
		healthpb.RegisterHealthServer(srv, health.NewServer())
	}

	// listen on internal socket
	if cfg.InternalSocketDir != "" {
		internalSocketDir := filepath.Dir(cfg.GitalyInternalSocketPath())
		sds, err := os.Stat(internalSocketDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				require.NoError(t, os.MkdirAll(internalSocketDir, 0700))
				t.Cleanup(func() { os.RemoveAll(internalSocketDir) })
			} else {
				require.FailNow(t, err.Error())
			}
		} else {
			require.True(t, sds.IsDir())
		}

		internalListener, err := net.Listen("unix", cfg.GitalyInternalSocketPath())
		require.NoError(t, err)
		go srv.Serve(internalListener)
	}

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

	go srv.Serve(listener)

	return srv, addr, gsd.disablePraefect
}

type gitalyServerDeps struct {
	disablePraefect bool
	logger          *logrus.Logger
	conns           *client.Pool
	locator         storage.Locator
	txMgr           transaction.Manager
	hookMgr         hook.Manager
	gitlabAPI       hook.GitlabAPI
	gitCmdFactory   git.CommandFactory
	linguist        *linguist.Instance
	backchannelReg  *backchannel.Registry
}

func (gsd *gitalyServerDeps) createDependencies(t testing.TB, cfg config.Cfg, rubyServer *rubyserver.Server) *service.Dependencies {
	if gsd.logger == nil {
		gsd.logger = testhelper.DiscardTestLogger(t)
	}

	if gsd.conns == nil {
		gsd.conns = client.NewPool()
	}

	if gsd.locator == nil {
		gsd.locator = config.NewLocator(cfg)
	}

	if gsd.gitlabAPI == nil {
		gsd.gitlabAPI = hook.GitlabAPIStub
	}

	if gsd.backchannelReg == nil {
		gsd.backchannelReg = backchannel.NewRegistry()
	}

	if gsd.txMgr == nil {
		gsd.txMgr = transaction.NewManager(cfg, gsd.backchannelReg)
	}

	if gsd.hookMgr == nil {
		gsd.hookMgr = hook.NewManager(gsd.locator, gsd.txMgr, gsd.gitlabAPI, cfg)
	}

	if gsd.gitCmdFactory == nil {
		gsd.gitCmdFactory = git.NewExecCommandFactory(cfg)
	}

	if gsd.linguist == nil {
		var err error
		gsd.linguist, err = linguist.New(cfg)
		require.NoError(t, err)
	}

	if gsd.backchannelReg == nil {
		gsd.backchannelReg = backchannel.NewRegistry()
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

// WithGitLabAPI sets hook.GitlabAPI instance that will be used for gitaly services initialisation.
func WithGitLabAPI(gitlabAPI hook.GitlabAPI) GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.gitlabAPI = gitlabAPI
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

// WithDisablePraefect disables setup and usage of the praefect as a proxy before the gitaly service.
func WithDisablePraefect() GitalyServerOpt {
	return func(deps gitalyServerDeps) gitalyServerDeps {
		deps.disablePraefect = true
		return deps
	}
}
