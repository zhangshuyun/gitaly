package remote_test

import (
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	serverPkg "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestSuccessfulFetchInternalRemote(t *testing.T) {
	gitaly0Dir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	gitaly1Dir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	defer func(oldStorages []config.Storage) { config.Config.Storages = oldStorages }(config.Config.Storages)
	config.Config.Storages = append(config.Config.Storages, []config.Storage{
		{
			Name: "gitaly-0",
			Path: gitaly0Dir,
		},
		{
			Name: "gitaly-1",
			Path: gitaly1Dir,
		},
	}...)

	gitaly0Server := testhelper.NewServer(t, nil, nil, testhelper.WithStorages([]string{"gitaly-0"}))
	gitalypb.RegisterSSHServiceServer(gitaly0Server.GrpcServer(), ssh.NewServer())
	gitalypb.RegisterRefServiceServer(gitaly0Server.GrpcServer(), ref.NewServer(config.NewLocator(config.Config)))
	reflection.Register(gitaly0Server.GrpcServer())
	require.NoError(t, gitaly0Server.Start())
	defer gitaly0Server.Stop()

	gitaly1Socket, cleanup := remote.RunRemoteServiceServer(t, testhelper.WithStorages([]string{"gitaly-1"}))
	defer cleanup()

	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	gitaly0Repo, gitaly0RepoPath, cleanup := cloneRepoAtStorage(t, repo, "gitaly-0")
	defer cleanup()

	gitaly1Repo, gitaly1RepoPath, cleanup := cloneRepoAtStorage(t, repo, "gitaly-1")
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", gitaly1RepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	client, conn := remote.NewRemoteClient(t, "unix://"+gitaly1Socket)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, err := helper.InjectGitalyServers(ctx, "gitaly-0", "unix://"+gitaly0Server.Socket(), "")
	require.NoError(t, err)

	c, err := client.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
		Repository:       gitaly1Repo,
		RemoteRepository: gitaly0Repo,
	})
	require.NoError(t, err)
	require.True(t, c.GetResult())

	require.Equal(t,
		string(testhelper.MustRunCommand(t, nil, "git", "-C", gitaly0RepoPath, "show-ref", "--head")),
		string(testhelper.MustRunCommand(t, nil, "git", "-C", gitaly1RepoPath, "show-ref", "--head")),
	)
}

func TestFailedFetchInternalRemote(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := remote.NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = metadata.NewOutgoingContext(ctx, md)

	// Non-existing remote repo
	remoteRepo := &gitalypb.Repository{StorageName: "default", RelativePath: "fake.git"}

	request := &gitalypb.FetchInternalRemoteRequest{
		Repository:       repo,
		RemoteRepository: remoteRepo,
	}

	c, err := client.FetchInternalRemote(ctx, request)
	require.NoError(t, err, "FetchInternalRemote is not supposed to return an error when 'git fetch' fails")
	require.False(t, c.GetResult())
}

func TestFailedFetchInternalRemoteDueToValidations(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := remote.NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}

	testCases := []struct {
		desc    string
		request *gitalypb.FetchInternalRemoteRequest
	}{
		{
			desc:    "empty Repository",
			request: &gitalypb.FetchInternalRemoteRequest{RemoteRepository: repo},
		},
		{
			desc:    "empty Remote Repository",
			request: &gitalypb.FetchInternalRemoteRequest{Repository: repo},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchInternalRemote(ctx, tc.request)

			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func runFullServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.NewInsecure(remote.RubyServer, nil, config.Config)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)

	return server, "unix://" + serverSocketPath
}

func cloneRepoAtStorage(t testing.TB, src *gitalypb.Repository, storageName string) (*gitalypb.Repository, string, func()) {
	dst := *src
	dst.StorageName = storageName

	dstP, err := helper.GetPath(&dst)
	require.NoError(t, err)

	srcP, err := helper.GetPath(src)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(dstP, 0755))
	testhelper.MustRunCommand(t, nil, "git",
		"clone", "--no-hardlinks", "--dissociate", "--bare", srcP, dstP)

	return &dst, dstP, func() { require.NoError(t, os.RemoveAll(dstP)) }
}
