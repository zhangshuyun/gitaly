package remote

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulFetchInternalRemote(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.AtomicFetch,
	}).Run(t, testSuccessfulFetchInternalRemote)
}

type mockHookManager struct {
	gitalyhook.Manager
	called int
}

func (m *mockHookManager) ReferenceTransactionHook(_ context.Context, _ gitalyhook.ReferenceTransactionState, _ []string, _ io.Reader) error {
	m.called++
	return nil
}

func testSuccessfulFetchInternalRemote(t *testing.T, ctx context.Context) {
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

	testhelper.ConfigureGitalyHooksBinary(config.Config.BinDir)

	hookManager := &mockHookManager{}
	gitaly0Addr := testserver.RunGitalyServer(t, config.Config, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(deps.GetCfg(), deps.GetLocator(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(deps.GetCfg(), deps.GetLocator(), deps.GetGitCmdFactory(), deps.GetTxManager()))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
	}, testserver.WithDisablePraefect())

	gitaly1Cfg := config.Config
	sockDir, cleanup := testhelper.TempDir(t)
	t.Cleanup(cleanup)
	gitaly1Cfg.InternalSocketDir = sockDir
	gitaly1Addr := testserver.RunGitalyServer(t, gitaly1Cfg, RubyServer, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(deps.GetCfg(), deps.GetRubyServer(), deps.GetLocator(), deps.GetGitCmdFactory()))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
	}, testserver.WithHookManager(hookManager), testserver.WithDisablePraefect())
	repo, _, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	locator := config.NewLocator(config.Config)

	gitaly0Repo, gitaly0RepoPath, cleanup := cloneRepoAtStorage(t, locator, repo, "gitaly-0")
	defer cleanup()

	gittest.CreateCommit(t, gitaly0RepoPath, "master", nil)

	gitaly1Repo, gitaly1RepoPath, cleanup := cloneRepoAtStorage(t, locator, repo, "gitaly-1")
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", gitaly1RepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	client, conn := newRemoteClient(t, gitaly1Addr)
	defer conn.Close()

	ctx, err := helper.InjectGitalyServers(ctx, "gitaly-0", gitaly0Addr, config.Config.Auth.Token)
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
			"GITALY_ADDRESS=" + gitaly0Addr,
		},
	)

	if featureflag.IsEnabled(ctx, featureflag.AtomicFetch) {
		require.Equal(t, 2, hookManager.called)
	} else {
		require.Equal(t, 0, hookManager.called)
	}
}

func TestFailedFetchInternalRemote(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := gittest.InitBareRepo(t)
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
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
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
