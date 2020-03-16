package ssh

import (
	"os"
	"path"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	testPath     = "testdata/scratch"
	testRepoRoot = testPath + "/data"
)

var (
	testRepo      *gitalypb.Repository
	gitalySSHPath string
	cwd           string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	hooks.Override = "/"
	cwd = mustGetCwd()

	err := os.RemoveAll(testPath)
	if err != nil {
		log.Fatal(err)
	}

	testRepo = testhelper.TestRepository()

	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()
	gitalySSHPath = path.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
}

func mustGetCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}
	return wd
}

func runSSHServer(t *testing.T, serverOpts ...ServerOpt) (string, func()) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), NewServer(serverOpts...))
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

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
