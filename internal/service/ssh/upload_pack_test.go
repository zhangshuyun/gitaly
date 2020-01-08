package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFailedUploadPackRequestDueToTimeout(t *testing.T) {
	stop, serverSocketPath := runSSHServer(t, WithUploadPackRequestTimeout(10*time.Microsecond))

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
	stop, serverSocketPath := runSSHServer(t)
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
			ctx, cancel := context.WithCancel(context.Background())
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
	stop, serverSocketPath := runSSHServer(t)
	defer stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local")

	tests := []struct {
		cmd  *exec.Cmd
		desc string
	}{
		{
			cmd:  exec.Command("git", "clone", "git@localhost:test/test.git", localRepoPath),
			desc: "full clone",
		},
		{
			cmd:  exec.Command("git", "clone", "--depth", "1", "git@localhost:test/test.git", localRepoPath),
			desc: "shallow clone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			lHead, rHead, _, _, err := testClone(t, serverSocketPath, testRepo.GetStorageName(), testRepo.GetRelativePath(), localRepoPath, "", tc.cmd, "")
			require.NoError(t, err, "clone failed")
			require.Equal(t, lHead, rHead, "local and remote head not equal")
		})
	}
}

func TestUploadPackCloneSuccessWithGitProtocol(t *testing.T) {
	restore := testhelper.EnableGitProtocolV2Support()
	defer restore()

	stop, serverSocketPath := runSSHServer(t)
	defer stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local")

	tests := []struct {
		cmd  *exec.Cmd
		desc string
	}{
		{
			cmd:  exec.Command("git", "clone", "git@localhost:test/test.git", localRepoPath),
			desc: "full clone",
		},
		{
			cmd:  exec.Command("git", "clone", "--depth", "1", "git@localhost:test/test.git", localRepoPath),
			desc: "shallow clone",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			lHead, rHead, _, _, err := testClone(t, serverSocketPath, testRepo.GetStorageName(), testRepo.GetRelativePath(), localRepoPath, "", tc.cmd, git.ProtocolV2)
			require.NoError(t, err, "clone failed")
			require.Equal(t, lHead, rHead, "local and remote head not equal")

			envData, err := testhelper.GetGitEnvData()

			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
		})
	}
}

func TestUploadPackCloneHideTags(t *testing.T) {
	stop, serverSocketPath := runSSHServer(t)
	defer stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local-hide-tags")

	cmd := exec.Command("git", "clone", "--mirror", "git@localhost:test/test.git", localRepoPath)

	_, _, lTags, rTags, err := testClone(t, serverSocketPath, testRepo.GetStorageName(), testRepo.GetRelativePath(), localRepoPath, "transfer.hideRefs=refs/tags", cmd, "")

	if err != nil {
		t.Fatalf("clone failed: %v", err)
	}
	if lTags == rTags {
		t.Fatalf("local and remote tags are equal. clone failed: %q != %q", lTags, rTags)
	}
	if tag := "v1.0.0"; !strings.Contains(rTags, tag) {
		t.Fatalf("sanity check failed, tag %q not found in %q", tag, rTags)
	}
}

func TestUploadPackCloneFailure(t *testing.T) {
	stop, serverSocketPath := runSSHServer(t)
	defer stop()

	localRepoPath := path.Join(testRepoRoot, "gitlab-test-upload-pack-local-failure")

	cmd := exec.Command("git", "clone", "git@localhost:test/test.git", localRepoPath)

	_, _, _, _, err := testClone(t, serverSocketPath, "foobar", testRepo.GetRelativePath(), localRepoPath, "", cmd, "")
	if err == nil {
		t.Fatalf("clone didn't fail")
	}
}

func testClone(t *testing.T, serverSocketPath, storageName, relativePath, localRepoPath string, gitConfig string, cmd *exec.Cmd, gitProtocol string) (string, string, string, string, error) {
	defer os.RemoveAll(localRepoPath)

	pbTempRepo := &gitalypb.Repository{StorageName: storageName, RelativePath: relativePath}
	req := &gitalypb.SSHUploadPackRequest{
		Repository:  pbTempRepo,
		GitProtocol: gitProtocol,
	}
	if gitConfig != "" {
		req.GitConfigOptions = strings.Split(gitConfig, " ")
	}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(req)

	require.NoError(t, err)

	cmd.Env = []string{
		fmt.Sprintf("GITALY_ADDRESS=%s", serverSocketPath),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-pack`, gitalySSHPath),
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", "", "", fmt.Errorf("%v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		return "", "", "", "", fmt.Errorf("Failed to run `git clone`: %q", out)
	}

	storagePath := testhelper.GitlabTestStoragePath()
	testRepoPath := path.Join(storagePath, testRepo.GetRelativePath())

	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "master"))
	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	remoteTags := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag"))
	localTags := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "tag"))

	return string(localHead), string(remoteHead), string(localTags), string(remoteTags), nil
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
