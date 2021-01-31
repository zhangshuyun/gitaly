package testserver

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	internalauth "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	gitalyserver "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var RubyServer = &rubyserver.Server{}

// PartialGitaly is a subset of Gitaly's behavior
type PartialGitaly interface {
	gitalypb.ServerServiceServer
	gitalypb.RepositoryServiceServer
	gitalypb.InternalGitalyServer
	gitalypb.CommitServiceServer
	healthpb.HealthServer
}

func registerGitalyServices(server *grpc.Server, pg PartialGitaly) {
	gitalypb.RegisterServerServiceServer(server, pg)
	gitalypb.RegisterRepositoryServiceServer(server, pg)
	gitalypb.RegisterInternalGitalyServer(server, pg)
	gitalypb.RegisterCommitServiceServer(server, pg)
	healthpb.RegisterHealthServer(server, pg)
}

// RealGitaly instantiates an instance of PartialGitaly that uses the real world
// services. This is intended to be used in integration tests to validate
// Gitaly services.
func RealGitaly(storages []config.Storage, authToken, internalSocketPath string) PartialGitaly {
	locator := config.NewLocator(config.Config)
	transactionManager := transaction.NewManager(config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	return struct {
		gitalypb.ServerServiceServer
		gitalypb.RepositoryServiceServer
		gitalypb.InternalGitalyServer
		gitalypb.CommitServiceServer
		healthpb.HealthServer
	}{
		gitalyserver.NewServer(storages),
		repository.NewServer(config.Config, RubyServer, locator, transactionManager, gitCmdFactory),
		internalgitaly.NewServer(config.Config.Storages),
		commit.NewServer(config.Config, locator),
		health.NewServer(),
	}
}

func RunInternalGitalyServer(t testing.TB, internalSocketPath string, storages []config.Storage, token string) (*grpc.Server, string, func()) {
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(internalauth.Config{Token: token})}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(internalauth.Config{Token: token})}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	internalListener, err := net.Listen("unix", internalSocketPath)
	require.NoError(t, err)

	registerGitalyServices(server, RealGitaly(storages, token, internalSocketPath))

	errQ := make(chan error)

	go func() { errQ <- server.Serve(listener) }()
	go func() { errQ <- server.Serve(internalListener) }()

	cleanup := func() {
		server.Stop()
		require.NoError(t, <-errQ)
		require.NoError(t, <-errQ)
	}

	return server, "unix://" + serverSocketPath, cleanup
}

// RunGitalyServer starts gitaly server based on the provided cfg.
// Returns connection address and a cleanup function.
func RunGitalyServer(t *testing.T, cfg config.Cfg, rubyServer *rubyserver.Server) (string, testhelper.Cleanup) {
	t.Helper()

	conns := client.NewPool()
	locator := config.NewLocator(cfg)
	txManager := transaction.NewManager(cfg)
	hookMgr := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)

	srv, err := server.New(cfg.TLS.CertPath != "", rubyServer, hookMgr, txManager, cfg, conns, locator, gitCmdFactory)
	require.NoError(t, err)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	//listen on internal socket
	internalListener, err := net.Listen("unix", cfg.GitalyInternalSocketPath())
	require.NoError(t, err)

	go srv.Serve(internalListener)
	go srv.Serve(listener)

	return "unix://" + serverSocketPath, func() {
		conns.Close()
		srv.Stop()
	}
}
