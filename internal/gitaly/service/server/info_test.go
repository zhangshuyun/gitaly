package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestGitalyServerInfo(t *testing.T) {
	cfg := testcfg.Build(t)

	cfg.Storages = append(cfg.Storages, config.Storage{Name: "broken", Path: "/does/not/exist"})

	addr := runServer(t, cfg, testserver.WithDisablePraefect())

	client := newServerClient(t, addr)

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, storage.WriteMetadataFile(cfg.Storages[0].Path))
	metadata, err := storage.ReadMetadataFile(cfg.Storages[0].Path)
	require.NoError(t, err)

	c, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
	require.NoError(t, err)

	require.Equal(t, version.GetVersion(), c.GetServerVersion())

	gitVersion, err := git.CurrentVersion(ctx, git.NewExecCommandFactory(cfg))
	require.NoError(t, err)
	require.Equal(t, gitVersion.String(), c.GetGitVersion())

	require.Len(t, c.GetStorageStatuses(), len(cfg.Storages))
	require.True(t, c.GetStorageStatuses()[0].Readable)
	require.True(t, c.GetStorageStatuses()[0].Writeable)
	require.NotEmpty(t, c.GetStorageStatuses()[0].FsType)
	require.Equal(t, uint32(1), c.GetStorageStatuses()[0].ReplicationFactor)

	require.False(t, c.GetStorageStatuses()[1].Readable)
	require.False(t, c.GetStorageStatuses()[1].Writeable)
	require.Equal(t, metadata.GitalyFilesystemID, c.GetStorageStatuses()[0].FilesystemId)
	require.Equal(t, uint32(1), c.GetStorageStatuses()[1].ReplicationFactor)
}

func runServer(t *testing.T, cfg config.Cfg, opts ...testserver.GitalyServerOpt) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterServerServiceServer(srv, NewServer(deps.GetGitCmdFactory(), deps.GetCfg().Storages))
	}, opts...)
}

func TestServerNoAuth(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "some"}}))

	addr := runServer(t, cfg)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewServerServiceClient(conn)
	_, err = client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func newServerClient(t *testing.T, serverSocketPath string) gitalypb.ServerServiceClient {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(testhelper.RepositoryAuthToken)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return gitalypb.NewServerServiceClient(conn)
}
