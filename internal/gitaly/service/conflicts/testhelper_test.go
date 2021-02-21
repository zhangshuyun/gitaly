package conflicts

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var RubyServer *rubyserver.Server

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()
	testhelper.ConfigureGitalyGit2Go(config.Config.BinDir)

	tempDir, err := ioutil.TempDir("", "gitaly")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(tempDir)

	// dir of the BinDir is a temp folder
	hooks.Override = filepath.Join(filepath.Dir(config.Config.BinDir), "/hooks")

	RubyServer = rubyserver.New(config.Config)
	if err := RubyServer.Start(); err != nil {
		log.Error(err)
		return 1
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runConflictsServer(t *testing.T) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)
	locator := config.NewLocator(config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)

	gitalypb.RegisterConflictsServiceServer(srv.GrpcServer(), NewServer(RubyServer, config.Config, locator, gitCmdFactory))
	reflection.Register(srv.GrpcServer())
	srv.Start(t)

	return "unix://" + srv.Socket(), srv.Stop
}

func NewConflictsClient(t *testing.T, serverSocketPath string) (gitalypb.ConflictsServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewConflictsServiceClient(conn), conn
}
