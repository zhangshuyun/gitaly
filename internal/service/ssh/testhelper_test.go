package ssh

import (
	"log"
	"net"
	"os"
	"path"
	"testing"
	"time"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	scratchDir = "testdata/scratch"
)

var (
	serverSocketPath = path.Join(scratchDir, "gitaly.sock")
)

func TestMain(m *testing.M) {
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
