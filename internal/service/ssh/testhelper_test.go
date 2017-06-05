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
	scratchDir   = "testdata/scratch"
	testRepoRoot = "testdata/data"
)

var (
	serverSocketPath string
	testRepoPath     string
	workDir          string
)

func TestMain(m *testing.M) {
	var err error
	testRepoPath = testhelper.GitlabTestRepoPath()
	workDir, err = os.Getwd()
	if err != nil {
		log.Panicf("can't get work directory: %v\n", err)
	}
	serverSocketPath = path.Join(scratchDir, "gitaly.sock")

	os.RemoveAll(testRepoRoot)
	if err := os.MkdirAll(testRepoRoot, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(testRepoRoot)

	os.RemoveAll(scratchDir)
	if err := os.MkdirAll(scratchDir, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(scratchDir)

	os.Exit(func() int {
		return m.Run()
	}())
}

func runSSHServer(t *testing.T) *grpc.Server {
	server := grpc.NewServer()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterSSHServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newSSHClient(t *testing.T) pb.SSHClient {
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

	return pb.NewSSHClient(conn)
}
