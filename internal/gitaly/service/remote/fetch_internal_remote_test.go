package remote

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// gitalySSHParams contains parameters used to exec 'gitaly-ssh' binary.
type gitalySSHParams struct {
	arguments   []string
	environment []string
}

// listenGitalySSHCalls creates a script that intercepts 'gitaly-ssh' binary calls.
// It replaces 'gitaly-ssh' with a interceptor script that calls actual binary after flushing env var and
// arguments used for the binary invocation. That information will be returned back to the caller
// after invocation of the returned anonymous function.
func listenGitalySSHCalls(t *testing.T, conf config.Cfg) func() gitalySSHParams {
	t.Helper()

	require.NotEmpty(t, conf.BinDir)
	initialPath := filepath.Join(conf.BinDir, "gitaly-ssh")
	updatedPath := initialPath + "-actual"
	require.NoError(t, os.Rename(initialPath, updatedPath))

	tmpDir := testhelper.TempDir(t)

	script := fmt.Sprintf(`#!/bin/bash

		# To omit possible problem with parallel run and a race for the file creation with '>'
		# this option is used, please checkout https://mywiki.wooledge.org/NoClobber for more details.
		set -eo noclobber

		env >%[1]q/environment
		echo "$@" >%[1]q/arguments

		exec %[2]q "$@"`, tmpDir, updatedPath)
	require.NoError(t, os.WriteFile(initialPath, []byte(script), 0o755))

	return func() gitalySSHParams {
		arguments := testhelper.MustReadFile(t, filepath.Join(tmpDir, "arguments"))
		environment := testhelper.MustReadFile(t, filepath.Join(tmpDir, "environment"))
		return gitalySSHParams{
			arguments:   strings.Split(string(arguments), " "),
			environment: strings.Split(string(environment), "\n"),
		}
	}
}

func TestFetchInternalRemote_successful(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	t.Cleanup(cancel)

	remoteCfg, remoteRepo, remoteRepoPath := testcfg.BuildWithRepo(t)
	testhelper.BuildGitalyHooks(t, remoteCfg)
	gittest.WriteCommit(t, remoteCfg, remoteRepoPath, gittest.WithBranch("master"))

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
	}, testserver.WithDisablePraefect())

	localCfg, localRepoProto, localRepoPath := testcfg.BuildWithRepo(t)
	localRepo := localrepo.NewTestRepo(t, localCfg, localRepoProto)
	testhelper.BuildGitalySSH(t, localCfg)
	testhelper.BuildGitalyHooks(t, localCfg)
	gittest.Exec(t, remoteCfg, "-C", localRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	referenceTransactionHookCalled := 0

	// We do not require the server's address, but it needs to be around regardless such that
	// `FetchInternalRemote` can reach the hook service which is injected via the config.
	testserver.RunGitalyServer(t, localCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
			deps.GetCfg(),
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
	}, testserver.WithHookManager(gitalyhook.NewMockManager(t, nil, nil, nil,
		func(t *testing.T, _ context.Context, _ gitalyhook.ReferenceTransactionState, _ []string, stdin io.Reader) error {
			// We need to discard stdin or otherwise the sending Goroutine may return an
			// EOF error and cause the test to fail.
			_, err := io.Copy(io.Discard, stdin)
			require.NoError(t, err)

			referenceTransactionHookCalled++
			return nil
		}),
	), testserver.WithDisablePraefect())

	ctx, err := storage.InjectGitalyServers(ctx, remoteRepo.GetStorageName(), remoteAddr, remoteCfg.Auth.Token)
	require.NoError(t, err)
	ctx = metadata.OutgoingToIncoming(ctx)

	getGitalySSHInvocationParams := listenGitalySSHCalls(t, localCfg)

	connsPool := client.NewPool()
	defer connsPool.Close()

	// Use the `assert` package such that we can get information about why hooks have failed via
	// the hook logs in case it did fail unexpectedly.
	assert.NoError(t, FetchInternalRemote(ctx, localCfg, connsPool, localRepo, remoteRepo))

	hookLogs := filepath.Join(localCfg.Logging.Dir, "gitaly_hooks.log")
	require.FileExists(t, hookLogs)
	require.Equal(t, "", string(testhelper.MustReadFile(t, hookLogs)))

	require.Equal(t,
		string(gittest.Exec(t, remoteCfg, "-C", remoteRepoPath, "show-ref", "--head")),
		string(gittest.Exec(t, localCfg, "-C", localRepoPath, "show-ref", "--head")),
	)

	sshParams := getGitalySSHInvocationParams()
	require.Equal(t, []string{"upload-pack", "gitaly", "git-upload-pack", "'/internal.git'\n"}, sshParams.arguments)
	require.Subset(t,
		sshParams.environment,
		[]string{
			"GIT_TERMINAL_PROMPT=0",
			"GIT_SSH_VARIANT=simple",
			"LANG=en_US.UTF-8",
			"GITALY_ADDRESS=" + remoteAddr,
		},
	)

	require.Equal(t, 2, referenceTransactionHookCalled)
}

func TestFetchInternalRemote_failure(t *testing.T) {
	t.Parallel()

	cfg, repoProto, _, _ := setupRemoteService(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	connsPool := client.NewPool()
	defer connsPool.Close()

	err := FetchInternalRemote(ctx, cfg, connsPool, repo, &gitalypb.Repository{
		StorageName:  repoProto.GetStorageName(),
		RelativePath: "does-not-exist.git",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fatal: Could not read from remote repository")
}
