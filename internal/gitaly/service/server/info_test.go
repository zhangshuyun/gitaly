package server

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	internalauth "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

func TestGitalyServerInfo(t *testing.T) {
	// Setup storage paths
	testStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "broken", Path: "/does/not/exist"},
	}

	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)
	config.Config.Storages = testStorages

	server, serverSocketPath := runServer(t, config.Config)
	defer server.Stop()

	client, conn := newServerClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, storage.WriteMetadataFile(testStorages[0].Path))
	metadata, err := storage.ReadMetadataFile(testStorages[0].Path)
	require.NoError(t, err)

	c, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)

	require.Equal(t, version.GetVersion(), c.GetServerVersion())

	gitVersion, err := git.Version(ctx, git.NewExecCommandFactory(config.Config))
	require.NoError(t, err)
	require.Equal(t, gitVersion, c.GetGitVersion())

	require.Len(t, c.GetStorageStatuses(), len(testStorages))
	require.True(t, c.GetStorageStatuses()[0].Readable)
	require.True(t, c.GetStorageStatuses()[0].Writeable)
	require.NotEmpty(t, c.GetStorageStatuses()[0].FsType)
	require.Equal(t, uint32(1), c.GetStorageStatuses()[0].ReplicationFactor)

	require.False(t, c.GetStorageStatuses()[1].Readable)
	require.False(t, c.GetStorageStatuses()[1].Writeable)
	require.Equal(t, metadata.GitalyFilesystemID, c.GetStorageStatuses()[0].FilesystemId)
	require.Equal(t, uint32(1), c.GetStorageStatuses()[1].ReplicationFactor)
}

func runServer(t *testing.T, cfg config.Cfg) (*grpc.Server, string) {
	authConfig := internalauth.Config{Token: testhelper.RepositoryAuthToken}
	streamInt := []grpc.StreamServerInterceptor{auth.StreamServerInterceptor(authConfig)}
	unaryInt := []grpc.UnaryServerInterceptor{auth.UnaryServerInterceptor(authConfig)}

	server := testhelper.NewTestGrpcServer(t, streamInt, unaryInt)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	gitalypb.RegisterServerServiceServer(server, NewServer(gitCmdFactory, cfg.Storages))
	reflection.Register(server)

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func TestServerNoAuth(t *testing.T) {
	srv, path := runServer(t, config.Config)
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
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewServerServiceClient(conn), conn
}
