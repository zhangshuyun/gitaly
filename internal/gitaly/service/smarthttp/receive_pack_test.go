package smarthttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	pconfig "gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	uploadPackCapabilities = "report-status side-band-64k agent=git/2.12.0"
)

func TestSuccessfulReceivePackRequest(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	cfg.GitlabShell.Dir = "/foo/bar/gitlab-shell"

	hookOutputFile, cleanup := gittest.CaptureHookEnv(t)
	defer cleanup()

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, cfg, nil)

	projectPath := "project/path"

	repo.GlProjectPath = projectPath
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlUsername: "user", GlId: "123", GlRepository: "project-456"}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "0049\x01000eunpack ok\n0019ok refs/heads/master\n0019ok refs/heads/branch\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	gittest.Exec(t, cfg, "-C", repoPath, "show", push.newHead)

	envData := testhelper.MustReadFile(t, hookOutputFile)
	payload, err := git.HooksPayloadFromEnv(strings.Split(string(envData), "\n"))
	require.NoError(t, err)

	// Compare the repository up front so that we can use require.Equal for
	// the remaining values.
	testhelper.ProtoEqual(t, repo, payload.Repo)
	payload.Repo = nil

	// If running tests with Praefect, then these would be set, but we have
	// no way of figuring out their actual contents. So let's just remove
	// that data, too.
	payload.Transaction = nil
	payload.Praefect = nil

	require.Equal(t, git.HooksPayload{
		BinDir:              cfg.BinDir,
		GitPath:             cfg.Git.BinPath,
		InternalSocket:      cfg.GitalyInternalSocketPath(),
		InternalSocketToken: cfg.Auth.Token,
		ReceiveHooksPayload: &git.ReceiveHooksPayload{
			UserID:   "123",
			Username: "user",
			Protocol: "http",
		},
		RequestedHooks: git.ReceivePackHooks,
		FeatureFlags:   featureflag.RawFromContext(ctx),
	}, payload)
}

func TestReceivePackHiddenRefs(t *testing.T) {
	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	repoProto.GlProjectPath = "project/path"

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	oldHead, err := repo.ResolveRevision(ctx, "HEAD~")
	require.NoError(t, err)
	newHead, err := repo.ResolveRevision(ctx, "HEAD")
	require.NoError(t, err)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	for _, ref := range []string{
		"refs/environments/1",
		"refs/merge-requests/1/head",
		"refs/merge-requests/1/merge",
		"refs/pipelines/1",
	} {
		t.Run(ref, func(t *testing.T) {
			request := &bytes.Buffer{}
			gittest.WritePktlineString(t, request, fmt.Sprintf("%s %s %s\x00 %s",
				oldHead, newHead, ref, uploadPackCapabilities))
			gittest.WritePktlineFlush(t, request)

			// The options passed are the same ones used when doing an actual push.
			revisions := strings.NewReader(fmt.Sprintf("^%s\n%s\n", oldHead, newHead))
			pack := gittest.ExecStream(t, cfg, revisions, "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")
			request.Write(pack)

			stream, err := client.PostReceivePack(ctx)
			require.NoError(t, err)

			response := doPush(t, stream, &gitalypb.PostReceivePackRequest{
				Repository: repoProto, GlUsername: "user", GlId: "123", GlRepository: "project-456",
			}, request)

			require.Contains(t, string(response), fmt.Sprintf("%s deny updating a hidden ref", ref))
		})
	}
}

func TestSuccessfulReceivePackRequestWithGitProtocol(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	readProto, cfg := gittest.EnableGitProtocolV2Support(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, cfg, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123", GitProtocol: git.ProtocolV2}
	doPush(t, stream, firstRequest, push.body)

	envData := readProto()
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	gittest.Exec(t, cfg, "-C", repoPath, "show", push.newHead)
}

func TestFailedReceivePackRequestWithGitOpts(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, cfg, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123", GitConfigOptions: []string{"receive.MaxInputSize=1"}}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "002e\x02fatal: pack exceeds maximum allowed size\n0081\x010028unpack unpack-objects abnormal exit\n0028ng refs/heads/master unpacker error\n0028ng refs/heads/branch unpacker error\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
}

func TestFailedReceivePackRequestDueToHooksFailure(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	hookDir := testhelper.TempDir(t)

	defer func(override string) {
		hooks.Override = override
	}(hooks.Override)
	hooks.Override = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(cfg), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	require.NoError(t, ioutil.WriteFile(filepath.Join(hooks.Path(cfg), "pre-receive"), hookContent, 0755))

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, cfg, nil)
	firstRequest := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123"}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "007d\x01000eunpack ok\n0033ng refs/heads/master pre-receive hook declined\n0033ng refs/heads/branch pre-receive hook declined\n00000000"
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

func newTestPush(t *testing.T, cfg config.Cfg, fileContents []byte) *pushData {
	_, repoPath, localCleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	defer localCleanup()

	oldHead, newHead := createCommit(t, cfg, repoPath, fileContents)

	// ReceivePack request is a packet line followed by a packet flush, then the pack file of the objects we want to push.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_uploading_data
	// We form the packet line the same way git executable does: https://github.com/git/git/blob/d1a13d3fcb252631361a961cb5e2bf10ed467cba/send-pack.c#L524-L527
	requestBuffer := &bytes.Buffer{}

	pkt := fmt.Sprintf("%s %s refs/heads/master\x00 %s", oldHead, newHead, uploadPackCapabilities)
	fmt.Fprintf(requestBuffer, "%04x%s", len(pkt)+4, pkt)

	pkt = fmt.Sprintf("%s %s refs/heads/branch", git.ZeroOID, newHead)
	fmt.Fprintf(requestBuffer, "%04x%s", len(pkt)+4, pkt)

	fmt.Fprintf(requestBuffer, "%s", pktFlushStr)

	// We need to get a pack file containing the objects we want to push, so we use git pack-objects
	// which expects a list of revisions passed through standard input. The list format means
	// pack the objects needed if I have oldHead but not newHead (think of it from the perspective of the remote repo).
	// For more info, check the man pages of both `git-pack-objects` and `git-rev-list --objects`.
	stdin := strings.NewReader(fmt.Sprintf("^%s\n%s\n", oldHead, newHead))

	// The options passed are the same ones used when doing an actual push.
	pack := gittest.ExecStream(t, cfg, stdin, "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")
	requestBuffer.Write(pack)

	return &pushData{newHead: newHead, body: requestBuffer}
}

// createCommit creates a commit on HEAD with a file containing the
// specified contents.
func createCommit(t *testing.T, cfg config.Cfg, repoPath string, fileContents []byte) (oldHead string, newHead string) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// The latest commit ID on the remote repo
	oldHead = text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master"))

	changedFile := "README.md"
	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, changedFile), fileContents, 0644))

	gittest.Exec(t, cfg, "-C", repoPath, "add", changedFile)
	gittest.Exec(t, cfg, "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", commitMsg)

	// The commit ID we want to push to the remote repo
	newHead = text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master"))

	return oldHead, newHead
}

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	rpcRequests := []gitalypb.PostReceivePackRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, GlId: "user-123"}, // Repository doesn't exist
		{Repository: nil, GlId: "user-123"}, // Repository is nil
		{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "path/to/repo"}, GlId: ""},                               // Empty GlId
		{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "path/to/repo"}, GlId: "user-123", Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			stream, err := client.PostReceivePack(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&rpcRequest))
			require.NoError(t, stream.CloseSend())

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestInvalidTimezone(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	_, localRepoPath, localCleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	defer localCleanup()

	head := text.ChompBytes(gittest.Exec(t, cfg, "-C", localRepoPath, "rev-parse", "HEAD"))
	tree := text.ChompBytes(gittest.Exec(t, cfg, "-C", localRepoPath, "rev-parse", "HEAD^{tree}"))

	buf := new(bytes.Buffer)
	buf.WriteString("tree " + tree + "\n")
	buf.WriteString("parent " + head + "\n")
	buf.WriteString("author Au Thor <author@example.com> 1313584730 +051800\n")
	buf.WriteString("committer Au Thor <author@example.com> 1313584730 +051800\n")
	buf.WriteString("\n")
	buf.WriteString("Commit message\n")
	commit := text.ChompBytes(gittest.ExecStream(t, cfg, buf, "-C", localRepoPath, "hash-object", "-t", "commit", "--stdin", "-w"))

	stdin := strings.NewReader(fmt.Sprintf("^%s\n%s\n", head, commit))
	pack := gittest.ExecStream(t, cfg, stdin, "-C", localRepoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")

	pkt := fmt.Sprintf("%s %s refs/heads/master\x00 %s", head, commit, "report-status side-band-64k agent=git/2.12.0")
	body := &bytes.Buffer{}
	fmt.Fprintf(body, "%04x%s%s", len(pkt)+4, pkt, pktFlushStr)
	body.Write(pack)

	var cleanup func()
	_, cleanup = gittest.CaptureHookEnv(t)
	defer cleanup()

	socket := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, socket, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)
	firstRequest := &gitalypb.PostReceivePackRequest{
		Repository:   repo,
		GlId:         "user-123",
		GlRepository: "project-456",
	}
	response := doPush(t, stream, firstRequest, body)

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
	gittest.Exec(t, cfg, "-C", repoPath, "show", commit)
}

func TestReceivePackFsck(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	head := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "HEAD"))

	// We're creating a new commit which has a root tree with duplicate entries. git-mktree(1)
	// allows us to create these trees just fine, but git-fsck(1) complains.
	commit := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(
			gittest.TreeEntry{OID: "4b825dc642cb6eb9a060e54bf8d69288fbee4904", Path: "dup", Mode: "040000"},
			gittest.TreeEntry{OID: "4b825dc642cb6eb9a060e54bf8d69288fbee4904", Path: "dup", Mode: "040000"},
		),
	)

	stdin := strings.NewReader(fmt.Sprintf("^%s\n%s\n", head, commit))
	pack := gittest.ExecStream(t, cfg, stdin, "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")

	var body bytes.Buffer
	gittest.WritePktlineString(t, &body, fmt.Sprintf("%s %s refs/heads/master\x00 %s", head, commit, "report-status side-band-64k agent=git/2.12.0"))
	gittest.WritePktlineFlush(t, &body)
	_, err := body.Write(pack)
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	client, conn := newSmartHTTPClient(t, runSmartHTTPServer(t, cfg), cfg.Auth.Token)
	defer conn.Close()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	response := doPush(t, stream, &gitalypb.PostReceivePackRequest{
		Repository:   repo,
		GlId:         "user-123",
		GlRepository: "project-456",
	}, &body)

	require.Contains(t, string(response), "duplicateEntries: contains duplicate file entries")
}

func drainPostReceivePackResponse(stream gitalypb.SmartHTTPService_PostReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}

func TestPostReceivePackToHooks(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	const (
		secretToken  = "secret token"
		glRepository = "some_repo"
		glID         = "key-123"
	)

	var cleanup func()
	cfg.GitlabShell.Dir = testhelper.TempDir(t)

	cfg.Auth.Token = "abc123"
	cfg.Gitlab.SecretFile = testhelper.WriteShellSecretFile(t, cfg.GitlabShell.Dir, secretToken)

	push := newTestPush(t, cfg, nil)
	testRepoPath := filepath.Join(cfg.Storages[0].Path, repo.RelativePath)
	oldHead := text.ChompBytes(gittest.Exec(t, cfg, "-C", testRepoPath, "rev-parse", "HEAD"))

	changes := fmt.Sprintf("%s %s refs/heads/master\n", oldHead, push.newHead)

	cfg.Gitlab.URL, cleanup = testhelper.NewGitlabTestServer(t, testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "http",
	})
	defer cleanup()

	gittest.WriteCheckNewObjectExistsHook(t, cfg.Git.BinPath, testRepoPath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	socket := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, "unix://"+socket, cfg.Auth.Token)
	defer conn.Close()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	firstRequest := &gitalypb.PostReceivePackRequest{
		Repository:   repo,
		GlId:         glID,
		GlRepository: glRepository,
	}

	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "0049\x01000eunpack ok\n0019ok refs/heads/master\n0019ok refs/heads/branch\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
	require.Equal(t, io.EOF, drainPostReceivePackResponse(stream))
}

func TestPostReceiveWithTransactionsViaPraefect(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
	}).Run(t, testPostReceiveWithTransactionsViaPraefect)
}

func testPostReceiveWithTransactionsViaPraefect(t *testing.T, ctx context.Context) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	secretToken := "secret token"
	glID := "key-1234"
	glRepository := "some_repo"
	gitlabUser := "gitlab_user-1234"
	gitlabPassword := "gitlabsecret9887"

	opts := testhelper.GitlabTestServerOptions{
		User:         gitlabUser,
		Password:     gitlabPassword,
		SecretToken:  secretToken,
		GLID:         glID,
		GLRepository: glRepository,
		RepoPath:     repoPath,
	}

	serverURL, cleanup := testhelper.NewGitlabTestServer(t, opts)
	defer cleanup()

	gitlabShellDir := testhelper.TempDir(t)
	cfg.GitlabShell.Dir = gitlabShellDir
	cfg.Gitlab.URL = serverURL
	cfg.Gitlab.HTTPSettings.User = gitlabUser
	cfg.Gitlab.HTTPSettings.Password = gitlabPassword
	cfg.Gitlab.SecretFile = filepath.Join(gitlabShellDir, ".gitlab_shell_secret")

	testhelper.WriteShellSecretFile(t, gitlabShellDir, secretToken)

	addr := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, addr, cfg.Auth.Token)
	defer conn.Close()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, cfg, nil)
	request := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: glID, GlRepository: glRepository}
	response := doPush(t, stream, request, push.body)

	expectedResponse := "0049\x01000eunpack ok\n0019ok refs/heads/master\n0019ok refs/heads/branch\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
}

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	called int
}

func (t *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	t.called++
	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

func TestPostReceiveWithReferenceTransactionHook(t *testing.T) {
	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	refTransactionServer := &testTransactionServer{}

	addr := testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSmartHTTPServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetDiskCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
	}, testserver.WithDisablePraefect())

	// As we ain't got a Praefect server setup, we instead hooked up the
	// RefTransaction server for Gitaly itself. As this is the only Praefect
	// service required in this context, we can just pretend that
	// Gitaly is the Praefect server and inject it.
	praefectServer, err := txinfo.PraefectFromConfig(pconfig.Config{
		SocketPath: addr,
	})
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, err = txinfo.InjectTransaction(ctx, 1234, "primary", true)
	require.NoError(t, err)
	ctx, err = praefectServer.Inject(ctx)
	require.NoError(t, err)

	ctx = helper.IncomingToOutgoing(ctx)

	client := newMuxedSmartHTTPClient(t, ctx, addr, cfg.Auth.Token, func() backchannel.Server {
		srv := grpc.NewServer()
		gitalypb.RegisterRefTransactionServer(srv, refTransactionServer)
		return srv
	})

	t.Run("update", func(t *testing.T) {
		stream, err := client.PostReceivePack(ctx)
		require.NoError(t, err)

		repo, _, _ := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())

		request := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "key-1234", GlRepository: "some_repo"}
		response := doPush(t, stream, request, newTestPush(t, cfg, nil).body)

		expectedResponse := "0049\x01000eunpack ok\n0019ok refs/heads/master\n0019ok refs/heads/branch\n00000000"
		require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
		require.Equal(t, 4, refTransactionServer.called)
	})

	t.Run("delete", func(t *testing.T) {
		refTransactionServer.called = 0

		stream, err := client.PostReceivePack(ctx)
		require.NoError(t, err)

		repo, repoPath, _ := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())

		// Create a new branch which we're about to delete. We also pack references because
		// this used to generate two transactions: one for the packed-refs file and one for
		// the loose ref. We only expect a single transaction though, given that the
		// packed-refs transaction should get filtered out.
		gittest.Exec(t, cfg, "-C", repoPath, "branch", "delete-me")
		gittest.Exec(t, cfg, "-C", repoPath, "pack-refs", "--all")
		branchOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/delete-me"))

		uploadPackData := &bytes.Buffer{}
		gittest.WritePktlineString(t, uploadPackData, fmt.Sprintf("%s %s refs/heads/delete-me\x00 %s", branchOID, git.ZeroOID.String(), uploadPackCapabilities))
		gittest.WritePktlineFlush(t, uploadPackData)

		request := &gitalypb.PostReceivePackRequest{Repository: repo, GlId: "key-1234", GlRepository: "some_repo"}
		response := doPush(t, stream, request, uploadPackData)

		expectedResponse := "0033\x01000eunpack ok\n001cok refs/heads/delete-me\n00000000"
		require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)
		require.Equal(t, 2, refTransactionServer.called)
	})
}
