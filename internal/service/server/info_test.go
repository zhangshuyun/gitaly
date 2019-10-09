package server

import (
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	internalauth "gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

func TestGitalyServerInfo(t *testing.T) {
	server, serverSocketPath := runServer(t)
	defer server.Stop()

	client, conn := newServerClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	// Setup storage paths
	testStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "broken", Path: "/does/not/exist"},
	}
	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)
	config.Config.Storages = testStorages

	tempDir, err := ioutil.TempDir("", "gitaly-bin")
	require.NoError(t, err)

	config.Config.BinDir = tempDir

	require.NoError(t, storage.WriteMetadataFile(testStorages[0]))
	metadata, err := storage.ReadMetadataFile(testStorages[0])
	require.NoError(t, err)

	c, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)

	require.Equal(t, version.GetVersion(), c.GetServerVersion())

	gitVersion, err := git.Version()
	require.NoError(t, err)
	require.Equal(t, gitVersion, c.GetGitVersion())

	require.Len(t, c.GetStorageStatuses(), len(testStorages))
	require.True(t, c.GetStorageStatuses()[0].Readable)
	require.True(t, c.GetStorageStatuses()[0].Writeable)
	require.NotEmpty(t, c.GetStorageStatuses()[0].FsType)

	require.False(t, c.GetStorageStatuses()[1].Readable)
	require.False(t, c.GetStorageStatuses()[1].Writeable)
	require.Equal(t, metadata.GitalyFilesystemID, c.GetStorageStatuses()[0].FilesystemId)
}

func runServer(t *testing.T) (*grpc.Server, string) {
	authConfig := internalauth.Config{Token: testhelper.RepositoryAuthToken}
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(authConfig)}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(authConfig)}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterServerServiceServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func TestServerNoAuth(t *testing.T) {
	srv, path := runServer(t)
	defer srv.Stop()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(path, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewServerServiceClient(conn)
	_, err = client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func newServerClient(t *testing.T, serverSocketPath string) (gitalypb.ServerServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewServerServiceClient(conn), conn
}
