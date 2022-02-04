package ssh

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

type cloneCommand struct {
	command      git.Cmd
	repository   *gitalypb.Repository
	server       string
	featureFlags []string
	gitConfig    string
	gitProtocol  string
	cfg          config.Cfg
	sidechannel  bool
}

func runTestWithAndWithoutConfigOptions(t *testing.T, tf func(t *testing.T, opts ...testcfg.Option), opts ...testcfg.Option) {
	t.Run("no config options", func(t *testing.T) { tf(t) })

	if len(opts) > 0 {
		t.Run("with config options", func(t *testing.T) {
			tf(t, opts...)
		})
	}
}

func (cmd cloneCommand) execute(t *testing.T) error {
	req := &gitalypb.SSHUploadPackRequest{
		Repository:  cmd.repository,
		GitProtocol: cmd.gitProtocol,
	}
	if cmd.gitConfig != "" {
		req.GitConfigOptions = strings.Split(cmd.gitConfig, " ")
	}
	payload, err := protojson.Marshal(req)
	require.NoError(t, err)

	var flagPairs []string
	for _, flag := range cmd.featureFlags {
		flagPairs = append(flagPairs, fmt.Sprintf("%s:true", flag))
	}
	ctx := testhelper.Context(t)

	env := []string{
		fmt.Sprintf("GITALY_ADDRESS=%s", cmd.server),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_FEATUREFLAGS=%s", strings.Join(flagPairs, ",")),
		fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-pack`, filepath.Join(cmd.cfg.BinDir, "gitaly-ssh")),
	}
	if cmd.sidechannel {
		env = append(env, "GITALY_USE_SIDECHANNEL=1")
	}

	var output bytes.Buffer
	gitCommand, err := gittest.NewCommandFactory(t, cmd.cfg).NewWithoutRepo(ctx,
		cmd.command, git.WithStdout(&output), git.WithStderr(&output), git.WithEnv(env...), git.WithDisabledHooks(),
	)
	require.NoError(t, err)

	if err := gitCommand.Wait(); err != nil {
		return fmt.Errorf("Failed to run `git clone`: %q", output.Bytes())
	}

	return nil
}

func (cmd cloneCommand) test(t *testing.T, cfg config.Cfg, repoPath string, localRepoPath string) (string, string, string, string) {
	t.Helper()

	defer func() { require.NoError(t, os.RemoveAll(localRepoPath)) }()

	err := cmd.execute(t)
	require.NoError(t, err)

	remoteHead := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master"))
	localHead := text.ChompBytes(gittest.Exec(t, cfg, "-C", localRepoPath, "rev-parse", "master"))

	remoteTags := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "tag"))
	localTags := text.ChompBytes(gittest.Exec(t, cfg, "-C", localRepoPath, "tag"))

	return localHead, remoteHead, localTags, remoteTags
}

func TestFailedUploadPackRequestDueToTimeout(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testFailedUploadPackRequestDueToTimeout, testcfg.WithPackObjectsCacheEnabled())
}

func testFailedUploadPackRequestDueToTimeout(t *testing.T, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)

	cfg.SocketPath = runSSHServerWithOptions(t, cfg, []ServerOpt{WithUploadPackRequestTimeout(10 * time.Microsecond)})

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	client, conn := newSSHClient(t, cfg.SocketPath)
	defer conn.Close()
	ctx := testhelper.Context(t)

	stream, err := client.SSHUploadPack(ctx)
	require.NoError(t, err)

	// The first request is not limited by timeout, but also not under attacker control
	require.NoError(t, stream.Send(&gitalypb.SSHUploadPackRequest{Repository: repo}))

	// Because the client says nothing, the server would block. Because of
	// the timeout, it won't block forever, and return with a non-zero exit
	// code instead.
	requireFailedSSHStream(t, func() (int32, error) {
		resp, err := stream.Recv()
		if err != nil {
			return 0, err
		}

		var code int32
		if status := resp.GetExitStatus(); status != nil {
			code = status.Value
		}

		return code, nil
	})
}

func requireFailedSSHStream(t *testing.T, recv func() (int32, error)) {
	done := make(chan struct{})
	var code int32
	var err error

	go func() {
		for err == nil {
			code, err = recv()
		}
		close(done)
	}()

	select {
	case <-done:
		testhelper.RequireGrpcCode(t, err, codes.Internal)
		require.NotEqual(t, 0, code, "exit status")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for SSH stream")
	}
}

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	serverSocketPath := runSSHServer(t, cfg)

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *gitalypb.SSHUploadPackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: ""}},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: nil},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "path/to/repo"}, Stdin: []byte("Fail")},
			Code: func() codes.Code {
				if testhelper.IsPraefectEnabled() {
					return codes.NotFound
				}

				return codes.InvalidArgument
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx := testhelper.Context(t)
			stream, err := client.SSHUploadPack(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, stream.CloseSend())

			err = testPostUploadPackFailedResponse(t, stream)
			testhelper.RequireGrpcCode(t, err, test.Code)
		})
	}
}

func TestUploadPackCloneSuccess(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testUploadPackCloneSuccess, testcfg.WithPackObjectsCacheEnabled())
}

func testUploadPackCloneSuccess(t *testing.T, opts ...testcfg.Option) {
	testUploadPackCloneSuccess2(t, false, opts...)
}

func TestUploadPackWithSidechannelCloneSuccess(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testUploadPackWithSidechannelCloneSuccess, testcfg.WithPackObjectsCacheEnabled())
}

func testUploadPackWithSidechannelCloneSuccess(t *testing.T, opts ...testcfg.Option) {
	testUploadPackCloneSuccess2(t, true, opts...)
}

func testUploadPackCloneSuccess2(t *testing.T, sidechannel bool, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})

	cfg.SocketPath = runSSHServerWithOptions(t, cfg, []ServerOpt{WithPackfileNegotiationMetrics(negotiationMetrics)})

	repo, repoPath := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	localRepoPath := testhelper.TempDir(t)

	tests := []struct {
		cmd    git.Cmd
		desc   string
		deepen float64
	}{
		{
			cmd: git.SubCmd{
				Name: "clone",
				Args: []string{"git@localhost:test/test.git", localRepoPath},
			},
			desc:   "full clone",
			deepen: 0,
		},
		{
			cmd: git.SubCmd{
				Name: "clone",
				Flags: []git.Option{
					git.ValueFlag{Name: "--depth", Value: "1"},
				},
				Args: []string{"git@localhost:test/test.git", localRepoPath},
			},
			desc:   "shallow clone",
			deepen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			negotiationMetrics.Reset()

			cmd := cloneCommand{
				repository:  repo,
				command:     tc.cmd,
				server:      cfg.SocketPath,
				cfg:         cfg,
				sidechannel: sidechannel,
			}
			lHead, rHead, _, _ := cmd.test(t, cfg, repoPath, localRepoPath)
			require.Equal(t, lHead, rHead, "local and remote head not equal")

			metric, err := negotiationMetrics.GetMetricWithLabelValues("deepen")
			require.NoError(t, err)
			require.Equal(t, tc.deepen, promtest.ToFloat64(metric))
		})
	}
}

func TestUploadPackWithPackObjectsHook(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t, testcfg.WithPackObjectsCacheEnabled())

	filterDir := testhelper.TempDir(t)
	outputPath := filepath.Join(filterDir, "output")
	cfg.BinDir = filterDir

	testcfg.BuildGitalySSH(t, cfg)

	// We're using a custom pack-objetcs hook for git-upload-pack. In order
	// to assure that it's getting executed as expected, we're writing a
	// custom script which replaces the hook binary. It doesn't do anything
	// special, but writes an error message and errors out and should thus
	// cause the clone to fail with this error message.
	testhelper.WriteExecutable(t, filepath.Join(filterDir, "gitaly-hooks"), []byte(fmt.Sprintf(
		`#!/bin/bash
		set -eo pipefail
		echo 'I was invoked' >'%s'
		shift
		exec git "$@"
	`, outputPath)))

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	localRepoPath := testhelper.TempDir(t)

	err := cloneCommand{
		repository: repo,
		command: git.SubCmd{
			Name: "clone", Args: []string{"git@localhost:test/test.git", localRepoPath},
		},
		server: cfg.SocketPath,
		cfg:    cfg,
	}.execute(t)
	require.NoError(t, err)

	require.Equal(t, []byte("I was invoked\n"), testhelper.MustReadFile(t, outputPath))
}

func TestUploadPackWithoutSideband(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testUploadPackWithoutSideband, testcfg.WithPackObjectsCacheEnabled())
}

func testUploadPackWithoutSideband(t *testing.T, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	// While Git knows the side-band-64 capability, some other clients don't. There is no way
	// though to have Git not use that capability, so we're instead manually crafting a packfile
	// negotiation without that capability and send it along.
	negotiation := bytes.NewBuffer([]byte{})
	gittest.WritePktlineString(t, negotiation, "want 1e292f8fedd741b75372e19097c76d327140c312 multi_ack_detailed thin-pack include-tag ofs-delta agent=git/2.29.1")
	gittest.WritePktlineString(t, negotiation, "want 1e292f8fedd741b75372e19097c76d327140c312")
	gittest.WritePktlineFlush(t, negotiation)
	gittest.WritePktlineString(t, negotiation, "done")

	request := &gitalypb.SSHUploadPackRequest{
		Repository: repo,
	}
	payload, err := protojson.Marshal(request)
	require.NoError(t, err)

	// As we're not using the sideband, the remote process will write both to stdout and stderr.
	// Those simultaneous writes to both stdout and stderr created a race as we could've invoked
	// two concurrent `SendMsg`s on the gRPC stream. And given that `SendMsg` is not thread-safe
	// a deadlock would result.
	uploadPack := exec.Command(filepath.Join(cfg.BinDir, "gitaly-ssh"), "upload-pack", "dontcare", "dontcare")
	uploadPack.Env = []string{
		fmt.Sprintf("GITALY_ADDRESS=%s", cfg.SocketPath),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
	}
	uploadPack.Stdin = negotiation

	out, err := uploadPack.CombinedOutput()
	require.NoError(t, err)
	require.True(t, uploadPack.ProcessState.Success())
	require.Contains(t, string(out), "refs/heads/master")
	require.Contains(t, string(out), "Counting objects")
	require.Contains(t, string(out), "PACK")
}

func TestUploadPackCloneWithPartialCloneFilter(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testUploadPackCloneWithPartialCloneFilter, testcfg.WithPackObjectsCacheEnabled())
}

func testUploadPackCloneWithPartialCloneFilter(t *testing.T, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	// Ruby file which is ~1kB in size and not present in HEAD
	blobLessThanLimit := git.ObjectID("6ee41e85cc9bf33c10b690df09ca735b22f3790f")
	// Image which is ~100kB in size and not present in HEAD
	blobGreaterThanLimit := git.ObjectID("18079e308ff9b3a5e304941020747e5c39b46c88")

	tests := []struct {
		desc     string
		repoTest func(t *testing.T, repoPath string)
		cmd      git.SubCmd
	}{
		{
			desc: "full_clone",
			repoTest: func(t *testing.T, repoPath string) {
				gittest.RequireObjectExists(t, cfg, repoPath, blobGreaterThanLimit)
			},
			cmd: git.SubCmd{
				Name: "clone",
			},
		},
		{
			desc: "partial_clone",
			repoTest: func(t *testing.T, repoPath string) {
				gittest.RequireObjectNotExists(t, cfg, repoPath, blobGreaterThanLimit)
			},
			cmd: git.SubCmd{
				Name: "clone",
				Flags: []git.Option{
					git.ValueFlag{Name: "--filter", Value: "blob:limit=2048"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// Run the clone with filtering enabled in both runs. The only
			// difference is that in the first run, we have the
			// UploadPackFilter flag disabled.
			localPath := testhelper.TempDir(t)

			tc.cmd.Args = []string{"git@localhost:test/test.git", localPath}

			cmd := cloneCommand{
				repository: repo,
				command:    tc.cmd,
				server:     cfg.SocketPath,
				cfg:        cfg,
			}
			err := cmd.execute(t)
			defer func() { require.NoError(t, os.RemoveAll(localPath)) }()
			require.NoError(t, err, "clone failed")

			gittest.RequireObjectExists(t, cfg, localPath, blobLessThanLimit)
			tc.repoTest(t, localPath)
		})
	}
}

func TestUploadPackCloneSuccessWithGitProtocol(t *testing.T) {
	t.Parallel()

	runTestWithAndWithoutConfigOptions(t, testUploadPackCloneSuccessWithGitProtocol, testcfg.WithPackObjectsCacheEnabled())
}

func testUploadPackCloneSuccessWithGitProtocol(t *testing.T, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)
	ctx := testhelper.Context(t)

	gitCmdFactory, readProto := gittest.NewProtocolDetectingCommandFactory(ctx, t, cfg)

	cfg.SocketPath = runSSHServer(t, cfg, testserver.WithGitCommandFactory(gitCmdFactory))

	repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	localRepoPath := testhelper.TempDir(t)

	tests := []struct {
		cmd  git.Cmd
		desc string
	}{
		{
			cmd: git.SubCmd{
				Name: "clone",
				Args: []string{"git@localhost:test/test.git", localRepoPath},
			},
			desc: "full clone",
		},
		{
			cmd: git.SubCmd{
				Name: "clone",
				Args: []string{"git@localhost:test/test.git", localRepoPath},
				Flags: []git.Option{
					git.ValueFlag{Name: "--depth", Value: "1"},
				},
			},
			desc: "shallow clone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := cloneCommand{
				repository:  repo,
				command:     tc.cmd,
				server:      cfg.SocketPath,
				gitProtocol: git.ProtocolV2,
				cfg:         cfg,
			}

			lHead, rHead, _, _ := cmd.test(t, cfg, repoPath, localRepoPath)
			require.Equal(t, lHead, rHead, "local and remote head not equal")

			envData := readProto()
			require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
		})
	}
}

func TestUploadPackCloneHideTags(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, repoPath := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	localRepoPath := testhelper.TempDir(t)

	cloneCmd := cloneCommand{
		repository: repo,
		command: git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--mirror"},
			},
			Args: []string{"git@localhost:test/test.git", localRepoPath},
		},
		server:    cfg.SocketPath,
		gitConfig: "transfer.hideRefs=refs/tags",
		cfg:       cfg,
	}
	_, _, lTags, rTags := cloneCmd.test(t, cfg, repoPath, localRepoPath)

	if lTags == rTags {
		t.Fatalf("local and remote tags are equal. clone failed: %q != %q", lTags, rTags)
	}
	if tag := "v1.0.0"; !strings.Contains(rTags, tag) {
		t.Fatalf("sanity check failed, tag %q not found in %q", tag, rTags)
	}
}

func TestUploadPackCloneFailure(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	localRepoPath := testhelper.TempDir(t)

	cmd := cloneCommand{
		repository: &gitalypb.Repository{
			StorageName:  "foobar",
			RelativePath: repo.GetRelativePath(),
		},
		command: git.SubCmd{
			Name: "clone",
			Args: []string{"git@localhost:test/test.git", localRepoPath},
		},
		server: cfg.SocketPath,
		cfg:    cfg,
	}
	err := cmd.execute(t)
	require.Error(t, err, "clone didn't fail")
}

func TestUploadPackCloneGitFailure(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	cfg.SocketPath = runSSHServer(t, cfg)

	repo, _ := gittest.CreateRepository(testhelper.Context(t), t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	client, conn := newSSHClient(t, cfg.SocketPath)
	defer conn.Close()

	configPath := filepath.Join(cfg.Storages[0].Path, repo.RelativePath, "config")
	gitconfig, err := os.Create(configPath)
	require.NoError(t, err)

	// Writing an invalid config will allow repo to pass the `IsGitDirectory` check but still
	// trigger an error when git tries to access the repo.
	_, err = gitconfig.WriteString("Not a valid git config")
	require.NoError(t, err)

	require.NoError(t, gitconfig.Close())
	ctx := testhelper.Context(t)
	stream, err := client.SSHUploadPack(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err = stream.Send(&gitalypb.SSHUploadPackRequest{Repository: repo}); err != nil {
		t.Fatal(err)
	}
	require.NoError(t, stream.CloseSend())

	err = testPostUploadPackFailedResponse(t, stream)
	testhelper.RequireGrpcCode(t, err, codes.Internal)
	require.EqualError(t, err, "rpc error: code = Internal desc = cmd wait: exit status 128, stderr: \"fatal: bad config line 1 in file ./config\\n\"")
}

func testPostUploadPackFailedResponse(t *testing.T, stream gitalypb.SSHService_SSHUploadPackClient) error {
	var err error
	var res *gitalypb.SSHUploadPackResponse

	for err == nil {
		res, err = stream.Recv()
		require.Nil(t, res.GetStdout())
	}

	return err
}
