package ssh

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type cloneCommand struct {
	command      *exec.Cmd
	repository   *gitalypb.Repository
	server       string
	featureFlags []string
	gitConfig    string
	gitProtocol  string
}

func (cmd cloneCommand) execute(t *testing.T) error {
	req := &gitalypb.SSHUploadPackRequest{
		Repository:  cmd.repository,
		GitProtocol: cmd.gitProtocol,
	}
	if cmd.gitConfig != "" {
		req.GitConfigOptions = strings.Split(cmd.gitConfig, " ")
	}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(req)

	require.NoError(t, err)

	var flagPairs []string
	for _, flag := range cmd.featureFlags {
		flagPairs = append(flagPairs, fmt.Sprintf("%s:true", flag))
	}

	cmd.command.Env = []string{
		fmt.Sprintf("GITALY_ADDRESS=%s", cmd.server),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_FEATUREFLAGS=%s", strings.Join(flagPairs, ",")),
		fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-pack`, gitalySSHPath),
	}

	out, err := cmd.command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %q", err, out)
	}
	if !cmd.command.ProcessState.Success() {
		return fmt.Errorf("Failed to run `git clone`: %q", out)
	}

	return nil
}

func (cmd cloneCommand) test(t *testing.T, localRepoPath string) (string, string, string, string) {
	defer os.RemoveAll(localRepoPath)

	err := cmd.execute(t)
	require.NoError(t, err)

	storagePath := testhelper.GitlabTestStoragePath()
	testRepoPath := filepath.Join(storagePath, testRepo.GetRelativePath())

	remoteHead := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "master"))
	localHead := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	remoteTags := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag"))
	localTags := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "tag"))

	return localHead, remoteHead, localTags, remoteTags
}

func TestFailedUploadPackRequestDueToTimeout(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t, WithUploadPackRequestTimeout(10*time.Microsecond))
	defer stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.SSHUploadPack(ctx)
	require.NoError(t, err)

	// The first request is not limited by timeout, but also not under attacker control
	require.NoError(t, stream.Send(&gitalypb.SSHUploadPackRequest{Repository: testRepo}))

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
		require.Equal(t, io.EOF, err)
		require.NotEqual(t, 0, code, "exit status")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for SSH stream")
	}
}

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *gitalypb.SSHUploadPackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: ""}},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: nil},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &gitalypb.SSHUploadPackRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, Stdin: []byte("Fail")},
			Code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			stream, err := client.SSHUploadPack(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			stream.CloseSend()

			err = testPostUploadPackFailedResponse(t, stream)
			testhelper.RequireGrpcError(t, err, test.Code)
		})
	}
}

func TestUploadPackCloneSuccess(t *testing.T) {
	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})

	serverSocketPath, stop := runSSHServer(
		t, WithPackfileNegotiationMetrics(negotiationMetrics),
	)
	defer stop()

	localRepoPath := filepath.Join(testRepoRoot, "gitlab-test-upload-pack-local")

	tests := []struct {
		cmd    *exec.Cmd
		desc   string
		deepen float64
	}{
		{
			cmd:    exec.Command(command.GitPath(), "clone", "git@localhost:test/test.git", localRepoPath),
			desc:   "full clone",
			deepen: 0,
		},
		{
			cmd:    exec.Command(command.GitPath(), "clone", "--depth", "1", "git@localhost:test/test.git", localRepoPath),
			desc:   "shallow clone",
			deepen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := cloneCommand{
				repository: testRepo,
				command:    tc.cmd,
				server:     serverSocketPath,
			}
			lHead, rHead, _, _ := cmd.test(t, localRepoPath)
			require.Equal(t, lHead, rHead, "local and remote head not equal")

			metric, err := negotiationMetrics.GetMetricWithLabelValues("deepen")
			require.NoError(t, err)
			require.Equal(t, tc.deepen, promtest.ToFloat64(metric))
		})
	}
}

func TestUploadPackCloneWithPartialCloneFilter(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	// Ruby file which is ~1kB in size and not present in HEAD
	blobLessThanLimit := "6ee41e85cc9bf33c10b690df09ca735b22f3790f"
	// Image which is ~100kB in size and not present in HEAD
	blobGreaterThanLimit := "18079e308ff9b3a5e304941020747e5c39b46c88"

	tests := []struct {
		desc      string
		repoTest  func(t *testing.T, repoPath string)
		cloneArgs []string
	}{
		{
			desc: "full_clone",
			repoTest: func(t *testing.T, repoPath string) {
				testhelper.GitObjectMustExist(t, repoPath, blobGreaterThanLimit)
			},
			cloneArgs: []string{"clone", "git@localhost:test/test.git"},
		},
		{
			desc: "partial_clone",
			repoTest: func(t *testing.T, repoPath string) {
				testhelper.GitObjectMustNotExist(t, repoPath, blobGreaterThanLimit)
			},
			cloneArgs: []string{"clone", "--filter=blob:limit=2048", "git@localhost:test/test.git"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			// Run the clone with filtering enabled in both runs. The only
			// difference is that in the first run, we have the
			// UploadPackFilter flag disabled.
			localPath := filepath.Join(testRepoRoot, fmt.Sprintf("gitlab-test-upload-pack-local-%s", tc.desc))
			cmd := cloneCommand{
				repository: testRepo,
				command:    exec.Command(command.GitPath(), append(tc.cloneArgs, localPath)...),
				server:     serverSocketPath,
			}
			err := cmd.execute(t)
			defer os.RemoveAll(localPath)
			require.NoError(t, err, "clone failed")

			testhelper.GitObjectMustExist(t, localPath, blobLessThanLimit)
			tc.repoTest(t, localPath)
		})
	}
}

func TestUploadPackCloneSuccessWithGitProtocol(t *testing.T) {
	localRepoPath := filepath.Join(testRepoRoot, "gitlab-test-upload-pack-local")

	tests := []struct {
		cmd  *exec.Cmd
		desc string
	}{
		{
			cmd:  exec.Command(command.GitPath(), "clone", "git@localhost:test/test.git", localRepoPath),
			desc: "full clone",
		},
		{
			cmd:  exec.Command(command.GitPath(), "clone", "--depth", "1", "git@localhost:test/test.git", localRepoPath),
			desc: "shallow clone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			restore := testhelper.EnableGitProtocolV2Support(t)
			defer restore()

			serverSocketPath, stop := runSSHServer(t)
			defer stop()

			cmd := cloneCommand{
				repository:  testRepo,
				command:     tc.cmd,
				server:      serverSocketPath,
				gitProtocol: git.ProtocolV2,
			}

			lHead, rHead, _, _ := cmd.test(t, localRepoPath)
			require.Equal(t, lHead, rHead, "local and remote head not equal")

			envData, err := testhelper.GetGitEnvData()

			require.NoError(t, err)
			require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
		})
	}
}

func TestUploadPackCloneHideTags(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	localRepoPath := filepath.Join(testRepoRoot, "gitlab-test-upload-pack-local-hide-tags")

	cmd := cloneCommand{
		repository: testRepo,
		command:    exec.Command(command.GitPath(), "clone", "--mirror", "git@localhost:test/test.git", localRepoPath),
		server:     serverSocketPath,
		gitConfig:  "transfer.hideRefs=refs/tags",
	}
	_, _, lTags, rTags := cmd.test(t, localRepoPath)

	if lTags == rTags {
		t.Fatalf("local and remote tags are equal. clone failed: %q != %q", lTags, rTags)
	}
	if tag := "v1.0.0"; !strings.Contains(rTags, tag) {
		t.Fatalf("sanity check failed, tag %q not found in %q", tag, rTags)
	}
}

func TestUploadPackCloneFailure(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	localRepoPath := filepath.Join(testRepoRoot, "gitlab-test-upload-pack-local-failure")

	cmd := cloneCommand{
		repository: &gitalypb.Repository{
			StorageName:  "foobar",
			RelativePath: testRepo.GetRelativePath(),
		},
		command: exec.Command(command.GitPath(), "clone", "git@localhost:test/test.git", localRepoPath),
		server:  serverSocketPath,
	}
	err := cmd.execute(t)
	require.Error(t, err, "clone didn't fail")
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
