package blob

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var rubyServer *rubyserver.Server

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	if err := testhelper.ConfigureRuby(&config.Config); err != nil {
		log.Error(err)
		return 1
	}

	rubyServer = rubyserver.New(config.Config)
	if err := rubyServer.Start(); err != nil {
		log.Error(err)
		return 1
	}
	defer rubyServer.Stop()

	return m.Run()
}

func runBlobServer(t *testing.T, locator storage.Locator) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterBlobServiceServer(srv.GrpcServer(), NewServer(rubyServer, config.Config, locator, git.NewExecCommandFactory(config.Config)))
	reflection.Register(srv.GrpcServer())

	srv.Start(t)

	return srv.Stop, "unix://" + srv.Socket()
}

func newBlobClient(t *testing.T, serverSocketPath string) (gitalypb.BlobServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewBlobServiceClient(conn), conn
}
