package conflicts

import (
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

var RubyServer = &rubyserver.Server{}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	tempDir, err := ioutil.TempDir("", "gitaly")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	hooks.Override = tempDir + "/hooks"
	config.Config.InternalSocketDir = tempDir + "/sock"

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runConflictsServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterConflictsServiceServer(srv.GrpcServer(), NewServer(RubyServer))
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
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
