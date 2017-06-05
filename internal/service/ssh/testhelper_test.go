package ssh

import (
	"net"
	"os"
	"path"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

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
	serverSocketPath = path.Join(scratchDir, "gitaly.sock")
	testRepoPath     = ""
)

func TestMain(m *testing.M) {
	testRepoPath = testhelper.GitlabTestRepoPath()

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

	server := runSSHServer()
	defer server.Stop()

	os.Exit(func() int {
		return m.Run()
	}())
}

func runSSHServer() *grpc.Server {
	server := grpc.NewServer()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		log.Fatal(err)
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
