package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

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
			require.NoError(t, err)

			require.NoError(t, stream.Send(test.Req))
			require.NoError(t, stream.CloseSend())

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, test.Code)
		})
	}
}

func TestReceivePackPushSuccess(t *testing.T) {
	defer func(dir string) { config.Config.GitlabShell.Dir = dir }(config.Config.GitlabShell.Dir)
	config.Config.GitlabShell.Dir = "/foo/bar/gitlab-shell"

	hookOutputFile, cleanup := testhelper.CaptureHookEnv(t)
	defer cleanup()

	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	glRepository := "project-456"
	glProjectPath := "project/path"

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "user-123", glRepository: glRepository, glProjectPath: glProjectPath})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData, err := ioutil.ReadFile(hookOutputFile)
	require.NoError(t, err, "get git env data")

	for _, env := range []string{
		"GL_ID=user-123",
		fmt.Sprintf("GL_REPOSITORY=%s", glRepository),
		fmt.Sprintf("GL_PROJECT_PATH=%s", glProjectPath),
		"GL_PROTOCOL=ssh",
		"GITALY_GITLAB_SHELL_DIR=" + "/foo/bar/gitlab-shell",
	} {
		require.Contains(t, strings.Split(string(envData), "\n"), env)
	}
}

func TestReceivePackPushSuccessWithGitProtocol(t *testing.T) {
	restore := testhelper.EnableGitProtocolV2Support()
	defer restore()

	serverSocketPath, stop := runSSHServer(t)
	defer stop()

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
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	_, _, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: "foobar", glID: "1"})
	require.Error(t, err, "local and remote head equal. push did not fail")

	_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: ""})
	require.Error(t, err, "local and remote head equal. push did not fail")
}

func TestReceivePackPushHookFailure(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	hookDir, err := ioutil.TempDir("", "gitaly-tmp-hooks")
	require.NoError(t, err)
	defer os.RemoveAll(hookDir)

	defer func(old string) { hooks.Override = old }(hooks.Override)
	hooks.Override = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	ioutil.WriteFile(path.Join(hooks.Path(), "pre-receive"), hookContent, 0755)

	_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testRepo.GetStorageName(), glID: "1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "(pre-receive hook declined)")
}

func TestObjectPoolRefAdvertisementHidingSSH(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.SSHReceivePack(ctx)
	require.NoError(t, err)

	repo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(repo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, repo))
	defer pool.Remove(ctx)

	require.NoError(t, pool.Link(ctx, repo))

	commitID := testhelper.CreateCommit(t, pool.FullPath(), t.Name(), nil)

	// First request
	require.NoError(t, stream.Send(&gitalypb.SSHReceivePackRequest{
		Repository: &gitalypb.Repository{StorageName: "default", RelativePath: repo.GetRelativePath()}, GlId: "user-123",
	}))

	require.NoError(t, stream.Send(&gitalypb.SSHReceivePackRequest{Stdin: []byte("0000")}))
	require.NoError(t, stream.CloseSend())

	r := streamio.NewReader(func() ([]byte, error) {
		msg, err := stream.Recv()
		return msg.GetStdout(), err
	})

	var b bytes.Buffer
	_, err = io.Copy(&b, r)
	require.NoError(t, err)
	require.NotContains(t, b.String(), commitID+" .have")
}

func TestSSHReceivePackToHooks(t *testing.T) {
	secretToken := "secret token"
	glRepository := "some_repo"
	glID := "key-123"

	restore := testhelper.EnableGitProtocolV2Support()
	defer restore()

	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir(t)
	defer cleanup()

	defer func(gitlabShell config.GitlabShell) {
		config.Config.GitlabShell = gitlabShell
	}(config.Config.GitlabShell)

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	cloneDetails, cleanup := setupSSHClone(t)
	defer cleanup()

	ts := testhelper.NewGitlabTestServer(testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     fmt.Sprintf("%s %s refs/heads/master\n", string(cloneDetails.OldHead), string(cloneDetails.NewHead)),
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
	})
	defer ts.Close()

	testhelper.WriteTemporaryGitlabShellConfigFile(t, tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL})
	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	config.Config.Gitlab.URL = ts.URL
	config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")

	testhelper.WriteCustomHook(cloneDetails.RemoteRepoPath, "pre-receive", []byte(testhelper.CheckNewObjectExists))

	lHead, rHead, err := sshPush(t, cloneDetails, serverSocketPath, pushParams{
		storageName:  testRepo.GetStorageName(),
		glID:         glID,
		glRepository: glRepository,
		gitProtocol:  git.ProtocolV2,
	})
	require.NoError(t, err)
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData, err := testhelper.GetGitEnvData()

	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
}

// SSHCloneDetails encapsulates values relevant for a test clone
type SSHCloneDetails struct {
	LocalRepoPath, RemoteRepoPath, TempRepo string
	OldHead                                 []byte
	NewHead                                 []byte
}

// setupSSHClone sets up a test clone
func setupSSHClone(t *testing.T) (SSHCloneDetails, func()) {
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
	oldHead, newHead, success := makeCommit(t, localRepoPath)
	require.True(t, success)

	return SSHCloneDetails{
			OldHead:        oldHead,
			NewHead:        newHead,
			LocalRepoPath:  localRepoPath,
			RemoteRepoPath: remoteRepoPath,
			TempRepo:       tempRepo,
		}, func() {
			os.RemoveAll(remoteRepoPath)
			os.RemoveAll(localRepoPath)
		}
}

func sshPush(t *testing.T, cloneDetails SSHCloneDetails, serverSocketPath string, params pushParams) (string, string, error) {
	pbTempRepo := &gitalypb.Repository{StorageName: params.storageName, RelativePath: cloneDetails.TempRepo, GlProjectPath: params.glProjectPath}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHReceivePackRequest{
		Repository:       pbTempRepo,
		GlRepository:     params.glRepository,
		GlId:             params.glID,
		GitConfigOptions: params.gitConfigOptions,
		GitProtocol:      params.gitProtocol,
	})
	require.NoError(t, err)

	cmd := exec.Command("git", "-C", cloneDetails.LocalRepoPath, "push", "-v", "git@localhost:test/test.git", "master")
	cmd.Env = []string{
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_ADDRESS=%s", serverSocketPath),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s receive-pack`, gitalySSHPath),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("error pushing: %v: %q", err, out)
	}

	if !cmd.ProcessState.Success() {
		return "", "", fmt.Errorf("failed to run `git push`: %q", out)
	}

	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", cloneDetails.LocalRepoPath, "rev-parse", "master"))
	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", cloneDetails.RemoteRepoPath, "rev-parse", "master"))

	return string(localHead), string(remoteHead), nil
}

func testCloneAndPush(t *testing.T, serverSocketPath string, params pushParams) (string, string, error) {
	cloneDetails, cleanup := setupSSHClone(t)
	defer cleanup()

	return sshPush(t, cloneDetails, serverSocketPath, params)
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

	return oldHead, newHead, true
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
	glRepository     string
	glProjectPath    string
	gitConfigOptions []string
	gitProtocol      string
}
