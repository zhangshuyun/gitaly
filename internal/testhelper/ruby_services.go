package testhelper

import (
	"net"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var RubyServer *rubyserver.Server

// testMainRuby should be used inside of TestMain for test suites that require
// the ruby server
func TestMainRuby(m *testing.M) int {
	defer MustHaveNoChildProcess()

	var err error
	ConfigureRuby()
	RubyServer, err = rubyserver.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

// RunOpSvcServer will run a ruby-based operation service. The factory function
// must be provided as a param in order to avoid a cyclical dependency.
func RunOpSvcServer(t *testing.T, newOpSvc func(*rubyserver.Server) gitalypb.OperationServiceServer) (*grpc.Server, string) {
	grpcServer := NewTestGrpcServer(t, nil, nil)
	serverSocketPath := GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterOperationServiceServer(grpcServer, newOpSvc(RubyServer))
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, "unix://" + serverSocketPath
}

// NewOpSvcCli returns an insecure gRPC client for the operations service at
// the provided socket path.
func NewOpSvcCli(t *testing.T, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}
