package ssh

import (
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
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
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(config.Config.Ruby.Dir)

	config.Config.Ruby.Dir = filepath.Join("../../../ruby", "git-hooks")

	err := os.RemoveAll(testPath)
	if err != nil {
		log.Fatal(err)
	}

	testRepo = testhelper.TestRepository()

	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()
	gitalySSHPath = filepath.Join(config.Config.BinDir, "gitaly-ssh")

	return m.Run()
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
