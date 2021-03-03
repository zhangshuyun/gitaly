package ssh

import (
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	gitalySSHPath string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(config.Config.Ruby.Dir)

	cleanup := testhelper.Configure()
	defer cleanup()

	testhelper.ConfigureGitalyHooksBinary(config.Config.BinDir)
	testhelper.ConfigureGitalySSH(config.Config.BinDir)
	gitalySSHPath = filepath.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
}

func runSSHServer(t *testing.T, cfg config.Cfg, serverOpts ...ServerOpt) (string, func()) {
	locator := config.NewLocator(cfg)
	txManager := transaction.NewManager(cfg)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)

	srv := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(cfg))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), NewServer(cfg, locator, gitCmdFactory, serverOpts...))
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hookservice.NewServer(cfg, hookManager, gitCmdFactory))
	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func newSSHClient(t *testing.T, serverSocketPath string) (gitalypb.SSHServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewSSHServiceClient(conn), conn
}
