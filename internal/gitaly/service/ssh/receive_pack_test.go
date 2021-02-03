package ssh

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
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

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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
			ctx, cancel := testhelper.Context()
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

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, pushParams{
		storageName:   testhelper.DefaultStorageName,
		glID:          "123",
		glUsername:    "user",
		glRepository:  glRepository,
		glProjectPath: glProjectPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData, err := ioutil.ReadFile(hookOutputFile)
	require.NoError(t, err, "get git env data")

	payload, err := git.HooksPayloadFromEnv(strings.Split(string(envData), "\n"))
	require.NoError(t, err)

	// Compare the repository up front so that we can use require.Equal for
	// the remaining values.
	testhelper.ProtoEqual(t, &gitalypb.Repository{
		StorageName:   testhelper.DefaultStorageName,
		RelativePath:  "gitlab-test-ssh-receive-pack.git",
		GlProjectPath: glProjectPath,
		GlRepository:  glRepository,
	}, payload.Repo)
	payload.Repo = nil

	// If running tests with Praefect, then these would be set, but we have
	// no way of figuring out their actual contents. So let's just remove
	// that data, too.
	payload.Transaction = nil
	payload.Praefect = nil

	require.Equal(t, git.HooksPayload{
		BinDir:              config.Config.BinDir,
		GitPath:             config.Config.Git.BinPath,
		InternalSocket:      config.Config.GitalyInternalSocketPath(),
		InternalSocketToken: config.Config.Auth.Token,
		ReceiveHooksPayload: &git.ReceiveHooksPayload{
			UserID:   "123",
			Username: "user",
			Protocol: "ssh",
		},
	}, payload)
}

func TestReceivePackPushSuccessWithGitProtocol(t *testing.T) {
	restore := testhelper.EnableGitProtocolV2Support(t)
	defer restore()

	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, pushParams{
		storageName:  testhelper.DefaultStorageName,
		glRepository: "project-123",
		glID:         "1",
		gitProtocol:  git.ProtocolV2,
	})
	require.NoError(t, err)

	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData := testhelper.GetGitEnvData(t)
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

func TestReceivePackPushFailure(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	_, _, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: "foobar", glID: "1"})
	require.Error(t, err, "local and remote head equal. push did not fail")

	_, _, err = testCloneAndPush(t, serverSocketPath, pushParams{storageName: testhelper.DefaultStorageName, glID: ""})
	require.Error(t, err, "local and remote head equal. push did not fail")
}

func TestReceivePackPushHookFailure(t *testing.T) {
	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	hookDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	defer func(old string) { hooks.Override = old }(hooks.Override)
	hooks.Override = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(config.Config), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	ioutil.WriteFile(filepath.Join(hooks.Path(config.Config), "pre-receive"), hookContent, 0755)

	_, _, err := testCloneAndPush(t, serverSocketPath, pushParams{storageName: testhelper.DefaultStorageName, glID: "1"})
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

	pool, err := objectpool.NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), repo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
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

	restore := testhelper.EnableGitProtocolV2Support(t)
	defer restore()

	serverSocketPath, stop := runSSHServer(t)
	defer stop()

	tempGitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	defer func(gitlabShell config.GitlabShell) {
		config.Config.GitlabShell = gitlabShell
	}(config.Config.GitlabShell)

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	cloneDetails, cleanup := setupSSHClone(t)
	defer cleanup()

	serverURL, cleanup := testhelper.NewGitlabTestServer(t, testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     fmt.Sprintf("%s %s refs/heads/master\n", string(cloneDetails.OldHead), string(cloneDetails.NewHead)),
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
	})
	defer cleanup()

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	config.Config.Gitlab.URL = serverURL
	config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")

	cleanup = testhelper.WriteCheckNewObjectExistsHook(t, cloneDetails.RemoteRepoPath)
	defer cleanup()

	lHead, rHead, err := sshPush(t, cloneDetails, serverSocketPath, pushParams{
		storageName:  testhelper.DefaultStorageName,
		glID:         glID,
		glRepository: glRepository,
		gitProtocol:  git.ProtocolV2,
	})
	require.NoError(t, err)
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData := testhelper.GetGitEnvData(t)
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

// SSHCloneDetails encapsulates values relevant for a test clone
type SSHCloneDetails struct {
	LocalRepoPath, RemoteRepoPath, TempRepo string
	OldHead                                 []byte
	NewHead                                 []byte
}

// setupSSHClone sets up a test clone
func setupSSHClone(t *testing.T) (SSHCloneDetails, func()) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	storagePath := testhelper.GitlabTestStoragePath()
	tempRepo := "gitlab-test-ssh-receive-pack.git"
	testRepoPath := filepath.Join(storagePath, testRepo.GetRelativePath())
	remoteRepoPath := filepath.Join(storagePath, tempRepo)
	localRepoPath := filepath.Join(storagePath, "gitlab-test-ssh-receive-pack-local")
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
	pbTempRepo := &gitalypb.Repository{
		StorageName:   params.storageName,
		RelativePath:  cloneDetails.TempRepo,
		GlProjectPath: params.glProjectPath,
		GlRepository:  params.glRepository,
	}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHReceivePackRequest{
		Repository:       pbTempRepo,
		GlRepository:     params.glRepository,
		GlId:             params.glID,
		GlUsername:       params.glUsername,
		GitConfigOptions: params.gitConfigOptions,
		GitProtocol:      params.gitProtocol,
	})
	require.NoError(t, err)

	cmd := exec.Command(config.Config.Git.BinPath, "-C", cloneDetails.LocalRepoPath, "push", "-v", "git@localhost:test/test.git", "master")
	cmd.Env = []string{
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_ADDRESS=%s", serverSocketPath),
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
	glUsername       string
	glRepository     string
	glProjectPath    string
	gitConfigOptions []string
	gitProtocol      string
}
