package ssh

import (
	"bytes"
	"context"
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
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSSHServer(t, cfg)

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *gitalypb.SSHReceivePackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: ""}, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: nil, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Empty GlId",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: repo.GetRelativePath()}, GlId: ""},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &gitalypb.SSHReceivePackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: repo.GetRelativePath()}, GlId: "user-123", Stdin: []byte("Fail")},
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
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	cfg.GitlabShell.Dir = "/foo/bar/gitlab-shell"

	testhelper.ConfigureGitalySSHBin(t, cfg)

	hookOutputFile, cleanup := gittest.CaptureHookEnv(t)
	defer cleanup()

	serverSocketPath := runSSHServer(t, cfg)

	glRepository := "project-456"
	glProjectPath := "project/path"

	lHead, rHead, err := testCloneAndPush(t, cfg, cfg.Storages[0].Path, serverSocketPath, repo, pushParams{
		storageName:   cfg.Storages[0].Name,
		glID:          "123",
		glUsername:    "user",
		glRepository:  glRepository,
		glProjectPath: glProjectPath,
	})
	require.NoError(t, err)
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData := testhelper.MustReadFile(t, hookOutputFile)
	payload, err := git.HooksPayloadFromEnv(strings.Split(string(envData), "\n"))
	require.NoError(t, err)

	// Compare the repository up front so that we can use require.Equal for
	// the remaining values.
	testhelper.ProtoEqual(t, &gitalypb.Repository{
		StorageName:   cfg.Storages[0].Name,
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
		BinDir:              cfg.BinDir,
		GitPath:             cfg.Git.BinPath,
		InternalSocket:      cfg.GitalyInternalSocketPath(),
		InternalSocketToken: cfg.Auth.Token,
		ReceiveHooksPayload: &git.ReceiveHooksPayload{
			UserID:   "123",
			Username: "user",
			Protocol: "ssh",
		},
		RequestedHooks: git.ReceivePackHooks,
	}, payload)
}

func TestReceivePackPushSuccessWithGitProtocol(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalySSHBin(t, cfg)
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	readProto, cfg := gittest.EnableGitProtocolV2Support(t, cfg)

	serverSocketPath := runSSHServer(t, cfg)

	lHead, rHead, err := testCloneAndPush(t, cfg, cfg.Storages[0].Path, serverSocketPath, repo, pushParams{
		storageName:  testhelper.DefaultStorageName,
		glRepository: "project-123",
		glID:         "1",
		gitProtocol:  git.ProtocolV2,
	})
	require.NoError(t, err)

	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData := readProto()
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

func TestReceivePackPushFailure(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSSHServer(t, cfg)

	_, _, err := testCloneAndPush(t, cfg, cfg.Storages[0].Path, serverSocketPath, repo, pushParams{storageName: "foobar", glID: "1"})
	require.Error(t, err, "local and remote head equal. push did not fail")

	_, _, err = testCloneAndPush(t, cfg, cfg.Storages[0].Path, serverSocketPath, repo, pushParams{storageName: cfg.Storages[0].Name, glID: ""})
	require.Error(t, err, "local and remote head equal. push did not fail")
}

func TestReceivePackPushHookFailure(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalySSHBin(t, cfg)

	serverSocketPath := runSSHServer(t, cfg)

	hookDir := testhelper.TempDir(t)

	defer func(old string) { hooks.Override = old }(hooks.Override)
	hooks.Override = hookDir

	require.NoError(t, os.MkdirAll(hooks.Path(cfg), 0755))

	hookContent := []byte("#!/bin/sh\nexit 1")
	require.NoError(t, ioutil.WriteFile(filepath.Join(hooks.Path(cfg), "pre-receive"), hookContent, 0755))

	_, _, err := testCloneAndPush(t, cfg, cfg.Storages[0].Path, serverSocketPath, repo, pushParams{storageName: cfg.Storages[0].Name, glID: "1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "(pre-receive hook declined)")
}

func TestObjectPoolRefAdvertisementHidingSSH(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	serverSocketPath := runSSHServer(t, cfg)

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.SSHReceivePack(ctx)
	require.NoError(t, err)

	pool, err := objectpool.NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		git.NewExecCommandFactory(cfg),
		nil,
		repo.GetStorageName(),
		gittest.NewObjectPoolName(t),
	)
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, repo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.NoError(t, pool.Link(ctx, repo))

	commitID := gittest.WriteCommit(t, cfg, pool.FullPath(), gittest.WithBranch(t.Name()))

	// First request
	require.NoError(t, stream.Send(&gitalypb.SSHReceivePackRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: repo.GetRelativePath()}, GlId: "user-123",
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

func TestReceivePackTransactional(t *testing.T) {
	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	var votes int
	serverSocketPath := runSSHServer(t, cfg, testserver.WithTransactionManager(
		&transaction.MockManager{
			VoteFn: func(context.Context, txinfo.Transaction,
				txinfo.PraefectServer, voting.Vote,
			) error {
				votes++
				return nil
			},
		},
	))

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx, err := (&txinfo.PraefectServer{SocketPath: "whatever"}).Inject(ctx)
	require.NoError(t, err)
	ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = helper.IncomingToOutgoing(ctx)

	masterOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath,
		"rev-parse", "refs/heads/master"))
	masterParentOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master~"))

	type command struct {
		ref    string
		oldOID string
		newOID string
	}

	for _, tc := range []struct {
		desc          string
		writePackfile bool
		commands      []command
		expectedRefs  map[string]string
		expectedVotes int
	}{
		{
			desc:          "noop",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/master",
					oldOID: masterOID,
					newOID: masterOID,
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/master": masterOID,
			},
			expectedVotes: 2,
		},
		{
			desc:          "update",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/master",
					oldOID: masterOID,
					newOID: masterParentOID,
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/master": masterParentOID,
			},
			expectedVotes: 2,
		},
		{
			desc:          "creation",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/other",
					oldOID: git.ZeroOID.String(),
					newOID: masterOID,
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/other": masterOID,
			},
			expectedVotes: 2,
		},
		{
			desc: "deletion",
			commands: []command{
				{
					ref:    "refs/heads/other",
					oldOID: masterOID,
					newOID: git.ZeroOID.String(),
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/other": git.ZeroOID.String(),
			},
			expectedVotes: 2,
		},
		{
			desc:          "multiple commands",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/a",
					oldOID: git.ZeroOID.String(),
					newOID: masterOID,
				},
				{
					ref:    "refs/heads/b",
					oldOID: git.ZeroOID.String(),
					newOID: masterOID,
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/a": masterOID,
				"refs/heads/b": masterOID,
			},
			expectedVotes: 3,
		},
		{
			desc:          "refused recreation of branch",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/a",
					oldOID: git.ZeroOID.String(),
					newOID: masterParentOID,
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/a": masterOID,
			},
			expectedVotes: 1,
		},
		{
			desc:          "refused recreation and successful delete",
			writePackfile: true,
			commands: []command{
				{
					ref:    "refs/heads/a",
					oldOID: git.ZeroOID.String(),
					newOID: masterParentOID,
				},
				{
					ref:    "refs/heads/b",
					oldOID: masterOID,
					newOID: git.ZeroOID.String(),
				},
			},
			expectedRefs: map[string]string{
				"refs/heads/a": masterOID,
			},
			expectedVotes: 2,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			votes = 0

			var request bytes.Buffer
			for i, command := range tc.commands {
				// Only the first pktline contains capabilities.
				if i == 0 {
					gittest.WritePktlineString(t, &request, fmt.Sprintf("%s %s %s\000 %s",
						command.oldOID, command.newOID, command.ref,
						"report-status side-band-64k agent=git/2.12.0"))
				} else {
					gittest.WritePktlineString(t, &request, fmt.Sprintf("%s %s %s",
						command.oldOID, command.newOID, command.ref))
				}
			}
			gittest.WritePktlineFlush(t, &request)

			if tc.writePackfile {
				// We're lazy and simply send over all objects to simplify test
				// setup.
				pack := gittest.Exec(t, cfg, "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")
				request.Write(pack)
			}

			stream, err := client.SSHReceivePack(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.SSHReceivePackRequest{
				Repository: repoProto, GlId: "user-123",
			}))
			require.NoError(t, stream.Send(&gitalypb.SSHReceivePackRequest{
				Stdin: request.Bytes(),
			}))
			require.NoError(t, stream.CloseSend())
			require.Equal(t, io.EOF, drainPostReceivePackResponse(stream))

			for expectedRef, expectedOID := range tc.expectedRefs {
				actualOID, err := repo.ResolveRevision(ctx, git.Revision(expectedRef))

				if expectedOID == git.ZeroOID.String() {
					require.Equal(t, git.ErrReferenceNotFound, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, expectedOID, actualOID.String())
				}
			}
			require.Equal(t, tc.expectedVotes, votes)
		})
	}
}

func TestSSHReceivePackToHooks(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	const (
		secretToken  = "secret token"
		glRepository = "some_repo"
		glID         = "key-123"
	)

	readProto, cfg := gittest.EnableGitProtocolV2Support(t, cfg)

	tempGitlabShellDir := testhelper.TempDir(t)

	cfg.GitlabShell.Dir = tempGitlabShellDir

	cloneDetails, cleanup := setupSSHClone(t, cfg, cfg.Storages[0].Path, repo)
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

	cfg.Gitlab.URL = serverURL
	cfg.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")

	gittest.WriteCheckNewObjectExistsHook(t, cfg.Git.BinPath, cloneDetails.RemoteRepoPath)

	serverSocketPath := runSSHServer(t, cfg)

	lHead, rHead, err := sshPush(t, cfg, cloneDetails, serverSocketPath, pushParams{
		storageName:  cfg.Storages[0].Name,
		glID:         glID,
		glRepository: glRepository,
		gitProtocol:  git.ProtocolV2,
	})
	require.NoError(t, err)
	require.Equal(t, lHead, rHead, "local and remote head not equal. push failed")

	envData := readProto()
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

// SSHCloneDetails encapsulates values relevant for a test clone
type SSHCloneDetails struct {
	LocalRepoPath, RemoteRepoPath, TempRepo string
	OldHead                                 []byte
	NewHead                                 []byte
}

// setupSSHClone sets up a test clone
func setupSSHClone(t *testing.T, cfg config.Cfg, storagePath string, testRepo *gitalypb.Repository) (SSHCloneDetails, func()) {
	tempRepo := "gitlab-test-ssh-receive-pack.git"
	testRepoPath := filepath.Join(storagePath, testRepo.GetRelativePath())
	remoteRepoPath := filepath.Join(storagePath, tempRepo)
	localRepoPath := filepath.Join(storagePath, "gitlab-test-ssh-receive-pack-local")
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	if err := os.RemoveAll(remoteRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	gittest.Exec(t, cfg, "clone", "--bare", testRepoPath, remoteRepoPath)
	// Make a non-bare clone of the test repo to act as a local one
	if err := os.RemoveAll(localRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	gittest.Exec(t, cfg, "clone", remoteRepoPath, localRepoPath)

	// We need git thinking we're pushing over SSH...
	oldHead, newHead, success := makeCommit(t, cfg, localRepoPath)
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

func sshPush(t *testing.T, cfg config.Cfg, cloneDetails SSHCloneDetails, serverSocketPath string, params pushParams) (string, string, error) {
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

	cmd := exec.Command(cfg.Git.BinPath, "-C", cloneDetails.LocalRepoPath, "push", "-v", "git@localhost:test/test.git", "master")
	cmd.Env = []string{
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_ADDRESS=%s", serverSocketPath),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s receive-pack`, filepath.Join(cfg.BinDir, "gitaly-ssh")),
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("error pushing: %v: %q", err, out)
	}

	if !cmd.ProcessState.Success() {
		return "", "", fmt.Errorf("failed to run `git push`: %q", out)
	}

	localHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", cloneDetails.LocalRepoPath, "rev-parse", "master"))
	remoteHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", cloneDetails.RemoteRepoPath, "rev-parse", "master"))

	return string(localHead), string(remoteHead), nil
}

func testCloneAndPush(t *testing.T, cfg config.Cfg, storagePath, serverSocketPath string, testRepo *gitalypb.Repository, params pushParams) (string, string, error) {
	cloneDetails, cleanup := setupSSHClone(t, cfg, storagePath, testRepo)
	defer cleanup()

	return sshPush(t, cfg, cloneDetails, serverSocketPath, params)
}

// makeCommit creates a new commit and returns oldHead, newHead, success
func makeCommit(t *testing.T, cfg config.Cfg, localRepoPath string) ([]byte, []byte, bool) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	newFilePath := localRepoPath + "/foo.txt"

	// Create a tiny file and add it to the index
	require.NoError(t, ioutil.WriteFile(newFilePath, []byte("foo bar"), 0644))
	gittest.Exec(t, cfg, "-C", localRepoPath, "add", ".")

	// The latest commit ID on the remote repo
	oldHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", localRepoPath, "rev-parse", "master"))

	gittest.Exec(t, cfg, "-C", localRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", commitMsg)
	if t.Failed() {
		return nil, nil, false
	}

	// The commit ID we want to push to the remote repo
	newHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", localRepoPath, "rev-parse", "master"))

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
