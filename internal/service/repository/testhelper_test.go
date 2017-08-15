package repository

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Stamp taken from https://golang.org/pkg/time/#pkg-constants
const testTimeString = "200601021504.05"

var (
	testTime         = time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
	testRepo         = testhelper.TestRepository()
)

func newRepositoryClient(t *testing.T) (pb.RepositoryServiceClient, *grpc.ClientConn) {
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

	return pb.NewRepositoryServiceClient(conn), conn
}

func runRepoServer(t *testing.T) *grpc.Server {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterRepositoryServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func assertModTimeAfter(t *testing.T, afterTime time.Time, paths ...string) bool {
	// NOTE: Since some filesystems don't have sub-second precision on `mtime`
	//       we're rounding the times to seconds
	afterTime = afterTime.Round(time.Second)
	for _, path := range paths {
		s, err := os.Stat(path)
		assert.NoError(t, err)

		if !s.ModTime().Round(time.Second).After(afterTime) {
			t.Errorf("ModTime is not after afterTime: %q < %q", s.ModTime().Round(time.Second).String(), afterTime.String())
		}
	}
	return t.Failed()
}
