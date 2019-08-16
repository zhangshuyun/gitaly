package storage

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var testStorage config.Storage

func TestMain(m *testing.M) {
	configureTestStorage()
	os.Exit(m.Run())
}

func configureTestStorage() {
	storagePath, err := filepath.Abs("testdata/repositories/storage1")
	if err != nil {
		panic(err)
	}

	if err := os.RemoveAll(storagePath); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(storagePath, 0755); err != nil {
		panic(err)
	}

	testStorage = config.Storage{Name: "storage-will-be-deleted", Path: storagePath}

	config.ModifyStorages([]config.Storage{testStorage})
}

func runStorageServer(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterStorageServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func newStorageClient(t *testing.T, serverSocketPath string) (gitalypb.StorageServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewStorageServiceClient(conn), conn
}
