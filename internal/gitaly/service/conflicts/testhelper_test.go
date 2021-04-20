package conflicts

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	hook_service "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
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

	tempDir, err := ioutil.TempDir("", "gitaly")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(tempDir)

	defer func(old string) { hooks.Override = old }(hooks.Override)
	hooks.Override = filepath.Join(tempDir, "hooks")

	return m.Run()
}

func SetupConflictsServiceWithRuby(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, bare bool) (config.Cfg, *gitalypb.Repository, string, gitalypb.ConflictsServiceClient) {
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)

	var repo *gitalypb.Repository
	var repoPath string
	var cleanup testhelper.Cleanup
	if bare {
		repo, repoPath, cleanup = gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
		t.Cleanup(cleanup)
	} else {
		repo, repoPath, cleanup = gittest.CloneRepoWithWorktreeAtStorage(t, cfg.Storages[0])
		t.Cleanup(cleanup)
	}

	serverSocketPath, stop := runConflictsServer(t, cfg, rubySrv)
	t.Cleanup(stop)
	cfg.SocketPath = serverSocketPath

	client, conn := NewConflictsClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	return cfg, repo, repoPath, client
}

func SetupConflictsService(t testing.TB, bare bool) (config.Cfg, *gitalypb.Repository, string, gitalypb.ConflictsServiceClient) {
	cfg := testcfg.Build(t)

	return SetupConflictsServiceWithRuby(t, cfg, nil, bare)
}

func runConflictsServer(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(cfg))
	locator := config.NewLocator(cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)

	gitalypb.RegisterCommitServiceServer(srv.GrpcServer(), commit.NewServer(cfg, locator,
		gitCmdFactory, nil))
	gitalypb.RegisterConflictsServiceServer(srv.GrpcServer(), NewServer(rubySrv, cfg, locator, gitCmdFactory))
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), repository.NewServer(cfg, rubySrv, locator, txManager, gitCmdFactory))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), ssh.NewServer(cfg, locator, gitCmdFactory))
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook_service.NewServer(cfg, hookManager, gitCmdFactory))
	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func NewConflictsClient(t testing.TB, serverSocketPath string) (gitalypb.ConflictsServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewConflictsServiceClient(conn), conn
}
