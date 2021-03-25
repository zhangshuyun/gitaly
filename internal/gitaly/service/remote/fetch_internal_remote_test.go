package remote

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

type mockHookManager struct {
	gitalyhook.Manager
	called int
}

func (m *mockHookManager) ReferenceTransactionHook(_ context.Context, _ gitalyhook.ReferenceTransactionState, _ []string, _ io.Reader) error {
	m.called++
	return nil
}

// GitalySSHParams contains parameters used to exec 'gitaly-ssh' binary.
type GitalySSHParams struct {
	Args    []string
	EnvVars []string
}

// listenGitalySSHCalls creates a script that intercepts 'gitaly-ssh' binary calls.
// It replaces 'gitaly-ssh' with a interceptor script that calls actual binary after flushing env var and
// arguments used for the binary invocation. That information will be returned back to the caller
// after invocation of the returned anonymous function.
func listenGitalySSHCalls(t *testing.T, conf config.Cfg) func() []GitalySSHParams {
	t.Helper()

	if conf.BinDir == "" {
		assert.FailNow(t, "BinDir must be set")
		return func() []GitalySSHParams { return nil }
	}

	const envPrefix = "env-"
	const argsPrefix = "args-"

	initialPath := filepath.Join(conf.BinDir, "gitaly-ssh")
	updatedPath := initialPath + "-actual"
	require.NoError(t, os.Rename(initialPath, updatedPath))

	tmpDir, clean := testhelper.TempDir(t)
	t.Cleanup(clean)

	script := fmt.Sprintf(`
		#!/bin/sh

		# To omit possible problem with parallel run and a race for the file creation with '>'
		# this option is used, please checkout https://mywiki.wooledge.org/NoClobber for more details.
		set -o noclobber

		ENV_IDX=$(ls %[1]q | grep %[2]s | wc -l)
		env > "%[1]s/%[2]s$ENV_IDX"

		ARGS_IDX=$(ls %[1]q | grep %[3]s | wc -l)
		echo $@ > "%[1]s/%[3]s$ARGS_IDX"

		%[4]q "$@" 1>&1 2>&2
		exit $?`,
		tmpDir, envPrefix, argsPrefix, updatedPath)

	require.NoError(t, ioutil.WriteFile(initialPath, []byte(script), 0755))

	getSSHParams := func() []GitalySSHParams {
		var gitalySSHParams []GitalySSHParams
		err := filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			filename := filepath.Base(path)

			parseParams := func(prefix, delim string) error {
				if !strings.HasPrefix(filename, prefix) {
					return nil
				}

				idx, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(filename, prefix)))
				if err != nil {
					return err
				}

				if len(gitalySSHParams) < idx+1 {
					tmp := make([]GitalySSHParams, idx+1)
					copy(tmp, gitalySSHParams)
					gitalySSHParams = tmp
				}

				data, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}

				params := strings.Split(string(data), delim)

				switch prefix {
				case argsPrefix:
					gitalySSHParams[idx].Args = params
				case envPrefix:
					gitalySSHParams[idx].EnvVars = params
				}

				return nil
			}

			if err := parseParams(envPrefix, "\n"); err != nil {
				return err
			}

			if err := parseParams(argsPrefix, " "); err != nil {
				return err
			}

			return nil
		})
		assert.NoError(t, err)
		return gitalySSHParams
	}

	return getSSHParams
}

func TestSuccessfulFetchInternalRemote(t *testing.T) {
	defer func(oldConf config.Cfg) { config.Config = oldConf }(config.Config)

	getGitalySSHInvocationParams, cleanup := listenGitalySSHCalls(t, config.Config)
	defer cleanup()

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

	require.Equal(t, 2, hookManager.called)
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
