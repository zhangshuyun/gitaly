package ssh

import (
	"net"
	"os"
	"path"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	testPath     = "testdata"
	testRepoRoot = testPath + "/data"
)

var (
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
	workDir          string
	testRepo         *pb.Repository

	uploadPackPath  string
	receivePackPath string

	cwd string
)

func TestMain(m *testing.M) {
	cwd = mustGetCwd()

	err := os.RemoveAll(testPath)
	if err != nil {
		log.Fatal(err)
	}

	testRepo = testhelper.TestRepository()

	// Build the test-binary that we need
	os.Remove("gitaly-upload-pack")
	testhelper.MustRunCommand(nil, nil, "go", "build", "gitlab.com/gitlab-org/gitaly/internal/service/ssh/cmd/gitaly-upload-pack")
	defer os.Remove("gitaly-upload-pack")
	uploadPackPath = path.Join(cwd, "gitaly-upload-pack")

	os.Remove("gitaly-receive-pack")
	testhelper.MustRunCommand(nil, nil, "go", "build", "gitlab.com/gitlab-org/gitaly/internal/service/ssh/cmd/gitaly-receive-pack")
	defer os.Remove("gitaly-receive-pack")
	receivePackPath = path.Join(cwd, "gitaly-receive-pack")

	os.Exit(func() int {
		return m.Run()
	}())
}

func mustGetCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		log.Panic(err)
	}
	return wd
}

func runSSHServer(t *testing.T) *grpc.Server {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterSSHServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newSSHClient(t *testing.T) (pb.SSHServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewSSHServiceClient(conn), conn
}
