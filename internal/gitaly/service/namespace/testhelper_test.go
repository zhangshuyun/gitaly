package namespace

import (
	"net"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func setupNamespaceService(t testing.TB) (config.Cfg, gitalypb.NamespaceServiceClient) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "other"))
	t.Cleanup(cfgBuilder.Cleanup)
	cfg := cfgBuilder.Build(t)

	locator := config.NewLocator(cfg)
	server, serverSocketPath := runNamespaceServer(t, locator)
	t.Cleanup(server.Stop)

	client, conn := newNamespaceClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	return cfg, client
}

func runNamespaceServer(t testing.TB, locator storage.Locator) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterNamespaceServiceServer(server, NewServer(locator))
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func newNamespaceClient(t testing.TB, serverSocketPath string) (gitalypb.NamespaceServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewNamespaceServiceClient(conn), conn
}
