package remote_test

import (
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestSuccessfulFetchInternalRemote(t *testing.T) {
	defer func(oldConf config.Cfg) { config.Config = oldConf }(config.Config)

	conf, getGitalySSHInvocationParams, cleanup := testhelper.ListenGitalySSHCalls(t, config.Config)
	defer cleanup()

	config.Config = conf

	gitaly0Dir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	gitaly1Dir, cleanup := testhelper.TempDir(t)
	defer cleanup()

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

	testhelper.ConfigureGitalyHooksBinary()

	locator := config.NewLocator(config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	gitaly0Server := testhelper.NewServer(t, nil, nil, testhelper.WithStorages([]string{"gitaly-0"}))
	gitalypb.RegisterSSHServiceServer(gitaly0Server.GrpcServer(), ssh.NewServer(config.Config, locator, gitCmdFactory))
	gitalypb.RegisterRefServiceServer(gitaly0Server.GrpcServer(), ref.NewServer(config.Config, locator))
	reflection.Register(gitaly0Server.GrpcServer())
	gitaly0Server.Start(t)
	defer gitaly0Server.Stop()

	gitaly1Socket, cleanup := remote.RunRemoteServiceServer(t, testhelper.WithStorages([]string{"gitaly-1"}))
	defer cleanup()

	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	gitaly0Repo, gitaly0RepoPath, cleanup := cloneRepoAtStorage(t, locator, repo, "gitaly-0")
	defer cleanup()

	testhelper.CreateCommit(t, gitaly0RepoPath, "master", nil)

	gitaly1Repo, gitaly1RepoPath, cleanup := cloneRepoAtStorage(t, locator, repo, "gitaly-1")
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

	gitalySSHInvocationParams := getGitalySSHInvocationParams()
	require.Len(t, gitalySSHInvocationParams, 1)
	require.Equal(t, []string{"upload-pack", "gitaly", "git-upload-pack", "'/internal.git'\n"}, gitalySSHInvocationParams[0].Args)
	require.Subset(t,
		gitalySSHInvocationParams[0].EnvVars,
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_VARIANT=simple",
			"LANG=en_US.UTF-8",
			"GITALY_ADDRESS=unix://" + gitaly0Server.Socket(),
		},
	)
}

func TestFailedFetchInternalRemote(t *testing.T) {
	serverSocketPath, clean := runFullServer(t)
	defer clean()

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
	serverSocketPath, clean := runFullServer(t)
	defer clean()

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

func runFullServer(t *testing.T) (string, func()) {
	conns := client.NewPool()
	server := serverPkg.NewInsecure(remote.RubyServer, nil, config.Config, conns)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)

	return "unix://" + serverSocketPath, func() {
		conns.Close()
		server.Stop()
	}
}

func cloneRepoAtStorage(t testing.TB, locator storage.Locator, src *gitalypb.Repository, storageName string) (*gitalypb.Repository, string, func()) {
	dst := *src
	dst.StorageName = storageName

	dstP, err := locator.GetPath(&dst)
	require.NoError(t, err)

	srcP, err := locator.GetPath(src)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(dstP, 0755))
	testhelper.MustRunCommand(t, nil, "git",
		"clone", "--no-hardlinks", "--dissociate", "--bare", srcP, dstP)

	return &dst, dstP, func() { require.NoError(t, os.RemoveAll(dstP)) }
}
