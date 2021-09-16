package remote

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	tmpDir := testhelper.TempDir(t)

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

	require.NoError(t, ioutil.WriteFile(initialPath, []byte(script), 0o755))

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

				data, err := os.ReadFile(path)
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
	t.Parallel()
	remoteCfg, remoteRepo, remoteRepoPath := testcfg.BuildWithRepo(t)

	testhelper.BuildGitalyHooks(t, remoteCfg)

	remoteAddr := testserver.RunGitalyServer(t, remoteCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
	}, testserver.WithDisablePraefect())

	gittest.WriteCommit(t, remoteCfg, remoteRepoPath, gittest.WithBranch("master"))

	localCfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("gitaly-1"))

	localCfg, localRepos := localCfgBuilder.BuildWithRepoAt(t, "stub")
	localRepo := localRepos[0]

	testhelper.BuildGitalySSH(t, localCfg)
	testhelper.BuildGitalyHooks(t, localCfg)

	getGitalySSHInvocationParams := listenGitalySSHCalls(t, localCfg)

	hookManager := &mockHookManager{}
	localAddr := testserver.RunGitalyServer(t, localCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
	}, testserver.WithHookManager(hookManager), testserver.WithDisablePraefect())

	localRepoPath := filepath.Join(localCfg.Storages[0].Path, localRepo.GetRelativePath())
	gittest.Exec(t, remoteCfg, "-C", localRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	client, conn := newRemoteClient(t, localAddr)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := testhelper.Context()
	t.Cleanup(cancel)

	ctx, err := helper.InjectGitalyServers(ctx, remoteRepo.GetStorageName(), remoteAddr, "")
	require.NoError(t, err)

	//nolint:staticcheck
	c, err := client.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
		Repository:       localRepo,
		RemoteRepository: remoteRepo,
	})
	require.NoError(t, err)
	require.True(t, c.GetResult())

	require.Equal(t,
		string(gittest.Exec(t, remoteCfg, "-C", remoteRepoPath, "show-ref", "--head")),
		string(gittest.Exec(t, remoteCfg, "-C", localRepoPath, "show-ref", "--head")),
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
			"GITALY_ADDRESS=" + remoteAddr,
		},
	)

	require.Equal(t, 2, hookManager.called)
}

func TestFailedFetchInternalRemote(t *testing.T) {
	t.Parallel()
	cfg, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	// Non-existing remote repo
	remoteRepo := &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "fake.git"}

	request := &gitalypb.FetchInternalRemoteRequest{
		Repository:       repo,
		RemoteRepository: remoteRepo,
	}

	//nolint:staticcheck
	c, err := client.FetchInternalRemote(ctx, request)
	require.NoError(t, err, "FetchInternalRemote is not supposed to return an error when 'git fetch' fails")
	require.False(t, c.GetResult())
}

func TestFailedFetchInternalRemoteDueToValidations(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

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
			//nolint:staticcheck
			_, err := client.FetchInternalRemote(ctx, tc.request)

			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}
