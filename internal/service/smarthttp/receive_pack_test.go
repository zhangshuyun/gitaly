package smarthttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulReceivePackRequest(t *testing.T) {
	defer func(dir string) { config.Config.GitlabShell.Dir = dir }(config.Config.GitlabShell.Dir)
	config.Config.GitlabShell.Dir = "/foo/bar/gitlab-shell"

	hookOutputFile, cleanup := testhelper.CaptureHookEnv(t)
	defer cleanup()

	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-456"}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show", push.newHead)

	envData, err := ioutil.ReadFile(hookOutputFile)
	require.NoError(t, err, "get git env data")

	for _, env := range []string{
		"GL_ID=user-123",
		"GL_REPOSITORY=project-456",
		"GL_PROTOCOL=http",
		"GITALY_GITLAB_SHELL_DIR=" + "/foo/bar/gitlab-shell",
	} {
		require.Contains(t, strings.Split(string(envData), "\n"), env)
	}
}

func TestSuccessfulReceivePackRequestWithGitProtocol(t *testing.T) {
	restore := testhelper.EnableGitProtocolV2Support()
	defer restore()

	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123", GitProtocol: git.ProtocolV2}
	doPush(t, stream, firstRequest, push.body)

	envData, err := testhelper.GetGitEnvData()

	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show", push.newHead)
}

func TestFailedReceivePackRequestWithGitOpts(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123", GitConfigOptions: []string{"receive.MaxInputSize=1"}}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "002e\x02fatal: pack exceeds maximum allowed size\n0059\x010028unpack unpack-objects abnormal exit\n0028ng refs/heads/master unpacker error\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
}

func TestFailedReceivePackRequestDueToHooksFailure(t *testing.T) {
	hookDir, err := ioutil.TempDir("", "gitaly-tmp-hooks")
	require.NoError(t, err)
	defer os.RemoveAll(hookDir)

	defer func() { hooks.Override = "" }()
	hooks.Override = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	ioutil.WriteFile(path.Join(hooks.Path(), "pre-receive"), hookContent, 0755)

	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123"}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "004a\x01000eunpack ok\n0033ng refs/heads/master pre-receive hook declined\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
}

func doPush(t *testing.T, stream gitalypb.SmartHTTPService_PostReceivePackClient, firstRequest *gitalypb.PostReceivePackRequest, body io.Reader) []byte {
	require.NoError(t, stream.Send(firstRequest))

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.PostReceivePackRequest{Data: p})
	})
	_, err := io.Copy(sw, body)
	require.NoError(t, err)

	require.NoError(t, stream.CloseSend())

	responseBuffer := bytes.Buffer{}
	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	_, err = io.Copy(&responseBuffer, rr)
	require.NoError(t, err)

	return responseBuffer.Bytes()
}

type pushData struct {
	newHead string
	body    io.Reader
}

func newTestPush(t *testing.T, fileContents []byte) *pushData {
	_, repoPath, localCleanup := testhelper.NewTestRepoWithWorktree(t)
	defer localCleanup()

	oldHead, newHead := createCommit(t, repoPath, fileContents)

	// ReceivePack request is a packet line followed by a packet flush, then the pack file of the objects we want to push.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_uploading_data
	// We form the packet line the same way git executable does: https://github.com/git/git/blob/d1a13d3fcb252631361a961cb5e2bf10ed467cba/send-pack.c#L524-L527
	clientCapabilities := "report-status side-band-64k agent=git/2.12.0"
	pkt := fmt.Sprintf("%s %s refs/heads/master\x00 %s", oldHead, newHead, clientCapabilities)

	// We need to get a pack file containing the objects we want to push, so we use git pack-objects
	// which expects a list of revisions passed through standard input. The list format means
	// pack the objects needed if I have oldHead but not newHead (think of it from the perspective of the remote repo).
	// For more info, check the man pages of both `git-pack-objects` and `git-rev-list --objects`.
	stdin := strings.NewReader(fmt.Sprintf("^%s\n%s\n", oldHead, newHead))

	// The options passed are the same ones used when doing an actual push.
	pack := testhelper.MustRunCommand(t, stdin, "git", "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")

	// We chop the request into multiple small pieces to exercise the server code that handles
	// the stream sent by the client, so we use a buffer to read chunks of data in a nice way.
	requestBuffer := &bytes.Buffer{}
	fmt.Fprintf(requestBuffer, "%04x%s%s", len(pkt)+4, pkt, pktFlushStr)
	requestBuffer.Write(pack)

	return &pushData{newHead: newHead, body: requestBuffer}
}

// createCommit creates a commit on HEAD with a file containing the
// specified contents.
func createCommit(t *testing.T, repoPath string, fileContents []byte) (oldHead string, newHead string) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// The latest commit ID on the remote repo
	oldHead = text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "master"))

	changedFile := "README.md"
	require.NoError(t, ioutil.WriteFile(path.Join(repoPath, changedFile), fileContents, 0644))

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "add", changedFile)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", commitMsg)

	// The commit ID we want to push to the remote repo
	newHead = text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "master"))

	return oldHead, newHead
}

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	rpcRequests := []gitalypb.PostReceivePackRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, GlId: "user-123"}, // Repository doesn't exist
		{Repository: nil, GlId: "user-123"}, // Repository is nil
		{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, GlId: ""},                               // Empty GlId
		{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, GlId: "user-123", Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.PostReceivePack(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&rpcRequest))
			stream.CloseSend()

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestPostReceivePackToHooks(t *testing.T) {
	secretToken := "secret token"
	glRepository := "some_repo"
	glID := "key-123"

	server, socket := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, "unix://"+socket)
	defer conn.Close()

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir()
	defer cleanup()

	gitlabShellDir := config.Config.GitlabShell.Dir
	defer func() {
		config.Config.GitlabShell.Dir = gitlabShellDir
	}()

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	repo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	push := newTestPush(t, nil)
	oldHead := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "HEAD"))

	changes := fmt.Sprintf("%s %s refs/heads/master\n", oldHead, push.newHead)

	c := testhelper.GitlabServerConfig{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "http",
	}

	ts := testhelper.NewGitlabTestServer(c)
	defer ts.Close()

	testhelper.WriteTemporaryGitlabShellConfigFile(tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL})
	testhelper.WriteShellSecretFile(tempGitlabShellDir, secretToken)

	defer func(override string) {
		hooks.Override = override
	}(hooks.Override)

	cwd, err := os.Getwd()
	require.NoError(t, err)
	hookDir := filepath.Join(cwd, "../../../ruby", "git-hooks")
	hooks.Override = hookDir

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	firstRequest := &gitalypb.PostReceivePackRequest{
		Repository:   repo,
		GlId:         glID,
		GlRepository: glRepository,
	}

	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
}

func drainPostReceivePackResponse(stream gitalypb.SmartHTTPService_PostReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
