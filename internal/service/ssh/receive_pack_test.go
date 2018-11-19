package ssh

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *gitalypb.SSHReceivePackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: ""}, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: nil, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Empty GlId",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: testRepo.GetRelativePath()}, GlId: ""},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: testRepo.GetRelativePath()}, GlId: "user-123", Stdin: []byte("Fail")},
			Code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SSHReceivePack(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			stream.CloseSend()

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, test.Code)
		})
	}
}

func TestReceivePackPushSuccess(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	lHead, rHead, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1", gitConfigOptions: []string{"receive.MaxInputSize=10000"}})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")
}

func TestReceivePackPushSuccessWithGitProtocol(t *testing.T) {
	defer func(old string) {
		config.Config.Git.BinPath = old
	}(config.Config.Git.BinPath)
	config.Config.Git.BinPath = "../../testhelper/env_git"

	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1", gitProtocol: git.ProtocolV2})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData, err := testhelper.GetGitEnvData()

	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
}

func TestReceivePackPushFailure(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	_, _, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: "foobar", glID: "1"})
	if err == nil {
		t.Errorf("local and remote head equal. push did not fail")
	}

	_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: ""})
	if err == nil {
		t.Errorf("local and remote head equal. push did not fail")
	}

	currentGitVersion, _ := git.Version()

	// receive.MaxInputSize is only available since Git 2.11.0
	// Skip this test from the job that uses Git 2.9.0
	if currentGitVersion != "2.9.0" {
		_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1", gitConfigOptions: []string{"receive.MaxInputSize=1"}})
		if err == nil {
			t.Errorf("local and remote head equal. push did not fail")
		}
	}

}

func TestReceivePackPushHookFailure(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	hookDir, err := ioutil.TempDir("", "gitaly-tmp-hooks")
	require.NoError(t, err)
	defer os.RemoveAll(hookDir)

	defer func(oldDir string) {
		config.Config.GitlabShell.Dir = oldDir
	}(config.Config.GitlabShell.Dir)
	config.Config.GitlabShell.Dir = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	ioutil.WriteFile(path.Join(hooks.Path(), "pre-receive"), hookContent, 0755)

	_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "(pre-receive hook declined)")
}

func testCloneAndPush(t *testing.T, serverSocketPath string, params pushParams) (string, string, error) {
	storagePath := testhelper.GitlabTestStoragePath()
	tempRepo := "gitlab-test-ssh-receive-pack.git"
	testRepoPath := path.Join(storagePath, testRepo.GetRelativePath())
	remoteRepoPath := path.Join(storagePath, tempRepo)
	localRepoPath := path.Join(storagePath, "gitlab-test-ssh-receive-pack-local")
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	if err := os.RemoveAll(remoteRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, remoteRepoPath)
	// Make a non-bare clone of the test repo to act as a local one
	if err := os.RemoveAll(localRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	testhelper.MustRunCommand(t, nil, "git", "clone", remoteRepoPath, localRepoPath)
	// We need git thinking we're pushing over SSH...
	defer os.RemoveAll(remoteRepoPath)
	defer os.RemoveAll(localRepoPath)

	makeCommit(t, localRepoPath)

	pbTempRepo := &gitalypb.Repository{StorageName: params.storageName, RelativePath: tempRepo}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHReceivePackRequest{
		Repository:       pbTempRepo,
		GlRepository:     pbTempRepo.GetRelativePath(),
		GlId:             params.glID,
		GitConfigOptions: params.gitConfigOptions,
		GitProtocol:      params.gitProtocol,
	})
	require.NoError(t, err)

	cmd := exec.Command("git", "-C", localRepoPath, "push", "-v", "git@localhost:test/test.git", "master")
	cmd.Env = []string{
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_ADDRESS=unix:%s", serverSocketPath),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s receive-pack`, gitalySSHPath),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("Error pushing: %v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		return "", "", fmt.Errorf("Failed to run `git push`: %q", out)
	}

	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))
	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rev-parse", "master"))

	return string(localHead), string(remoteHead), nil
}

// makeCommit creates a new commit and returns oldHead, newHead, success
func makeCommit(t *testing.T, localRepoPath string) ([]byte, []byte, bool) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	newFilePath := localRepoPath + "/foo.txt"

	// Create a tiny file and add it to the index
	require.NoError(t, ioutil.WriteFile(newFilePath, []byte("foo bar"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "add", ".")

	// The latest commit ID on the remote repo
	oldHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", commitMsg)
	if t.Failed() {
		return nil, nil, false
	}

	// The commit ID we want to push to the remote repo
	newHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	return oldHead, newHead, t.Failed()
}

func drainPostReceivePackResponse(stream gitalypb.SSHService_SSHReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}

type pushParams struct {
	storageName      string
	glID             string
	gitConfigOptions []string
	gitProtocol      string
}
