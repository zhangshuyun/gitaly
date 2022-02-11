package repository

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestReplicateRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testReplicateRepository)
}

func testReplicateRepository(t *testing.T, ctx context.Context) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	client := newRepositoryClient(t, cfg, serverSocketPath)

	repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	// create a loose object to ensure snapshot replication is used
	blobData, err := text.RandomHex(10)
	require.NoError(t, err)
	blobID := text.ChompBytes(gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewBuffer([]byte(blobData))},
		"-C", repoPath, "hash-object", "-w", "--stdin",
	))

	// write info attributes
	attrFilePath := filepath.Join(repoPath, "info", "attributes")
	require.NoError(t, os.MkdirAll(filepath.Dir(attrFilePath), 0o755))
	attrData := []byte("*.pbxproj binary\n")
	require.NoError(t, os.WriteFile(attrFilePath, attrData, 0o644))

	// Write a modified gitconfig
	gittest.Exec(t, cfg, "-C", repoPath, "config", "please.replicate", "me")
	configData := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.Contains(t, string(configData), "[please]\n\treplicate = me\n")

	targetRepo := proto.Clone(repo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     repo,
	})
	require.NoError(t, err)

	targetRepoPath := filepath.Join(cfg.Storages[1].Path, targetRepo.GetRelativePath())
	gittest.Exec(t, cfg, "-C", targetRepoPath, "fsck")

	replicatedAttrFilePath := filepath.Join(targetRepoPath, "info", "attributes")
	replicatedAttrData := testhelper.MustReadFile(t, replicatedAttrFilePath)
	require.Equal(t, string(attrData), string(replicatedAttrData), "info/attributes files must match")

	replicatedConfigPath := filepath.Join(targetRepoPath, "config")
	replicatedConfigData := testhelper.MustReadFile(t, replicatedConfigPath)
	require.Equal(t, string(configData), string(replicatedConfigData), "config files must match")

	// create another branch
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     repo,
	})
	require.NoError(t, err)
	require.Equal(t,
		gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--hash", "--verify", "refs/heads/branch"),
		gittest.Exec(t, cfg, "-C", targetRepoPath, "show-ref", "--hash", "--verify", "refs/heads/branch"),
	)

	// if an unreachable object has been replicated, that means snapshot replication was used
	gittest.Exec(t, cfg, "-C", targetRepoPath, "cat-file", "-p", blobID)
}

func TestReplicateRepositoryTransactional(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testReplicateRepositoryTransactional)
}

func testReplicateRepositoryTransactional(t *testing.T, ctx context.Context) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	sourceRepo, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	targetRepo := proto.Clone(sourceRepo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	var votes []string
	txServer := testTransactionServer{
		vote: func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
			votes = append(votes, hex.EncodeToString(request.ReferenceUpdatesHash))
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_COMMIT,
			}, nil
		},
	}

	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)
	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	client := newMuxedRepositoryClient(t, ctx, cfg, serverSocketPath, backchannel.NewClientHandshaker(
		testhelper.NewDiscardingLogEntry(t),
		func() backchannel.Server {
			srv := grpc.NewServer()
			gitalypb.RegisterRefTransactionServer(srv, &txServer)
			return srv
		},
	))

	// The first invocation creates the repository via a snapshot given that it doesn't yet
	// exist.
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})
	require.NoError(t, err)

	// There is no gitattributes file, so we vote on the empty contents of that file.
	gitattributesVote := sha1.Sum([]byte{})
	// There is a gitconfig though, so the vote should reflect its contents.
	gitconfigVote := sha1.Sum(testhelper.MustReadFile(t, filepath.Join(sourceRepoPath, "config")))

	require.Equal(t, []string{
		// We cannot easily derive these first two votes: they are based on the complete
		// hashed contents of the unpacked repository. We thus just only assert that they
		// are always the first two entries and that they are the same by simply taking the
		// first vote twice here.
		votes[0],
		votes[0],
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
	}, votes)

	// We're about to change refs/heads/master, and thus the mirror-fetch will update it. The
	// vote should reflect that.
	oldOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", sourceRepoPath, "rev-parse", "refs/heads/master"))
	newOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", sourceRepoPath, "rev-parse", "refs/heads/master~"))
	replicationVote := sha1.Sum([]byte(fmt.Sprintf("%[1]s %[2]s refs/heads/master\n%[1]s %[2]s HEAD\n", oldOID, newOID)))

	// We're now changing a reference in the source repository such that we can observe changes
	// in the target repo.
	gittest.Exec(t, cfg, "-C", sourceRepoPath, "update-ref", "refs/heads/master", "refs/heads/master~")

	votes = nil

	// And the second invocation uses FetchInternalRemote.
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitconfigVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(gitattributesVote[:]),
		hex.EncodeToString(replicationVote[:]),
		hex.EncodeToString(replicationVote[:]),
	}, votes)
}

func TestReplicateRepositoryInvalidArguments(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	testCases := []struct {
		description   string
		input         *gitalypb.ReplicateRepositoryRequest
		expectedError string
	}{
		{
			description: "everything correct",
			input: &gitalypb.ReplicateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "praefect-internal-0",
					RelativePath: "/ab/cd/abcdef1234",
				},
				Source: &gitalypb.Repository{
					StorageName:  "praefect-internal-1",
					RelativePath: "/ab/cd/abcdef1234",
				},
			},
			expectedError: "",
		},
		{
			description: "empty repository",
			input: &gitalypb.ReplicateRepositoryRequest{
				Repository: nil,
				Source: &gitalypb.Repository{
					StorageName:  "praefect-internal-1",
					RelativePath: "/ab/cd/abcdef1234",
				},
			},
			expectedError: "repository cannot be empty",
		},
		{
			description: "empty source",
			input: &gitalypb.ReplicateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "praefect-internal-0",
					RelativePath: "/ab/cd/abcdef1234",
				},
				Source: nil,
			},
			expectedError: "repository cannot be empty",
		},
		{
			description: "source and repository have different relative paths",
			input: &gitalypb.ReplicateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "praefect-internal-0",
					RelativePath: "/ab/cd/abcdef1234",
				},
				Source: &gitalypb.Repository{
					StorageName:  "praefect-internal-1",
					RelativePath: "/ab/cd/abcdef4321",
				},
			},
			expectedError: "both source and repository should have the same relative path",
		},
		{
			description: "source and repository have the same storage",
			input: &gitalypb.ReplicateRepositoryRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "praefect-internal-0",
					RelativePath: "/ab/cd/abcdef1234",
				},
				Source: &gitalypb.Repository{
					StorageName:  "praefect-internal-0",
					RelativePath: "/ab/cd/abcdef1234",
				},
			},
			expectedError: "repository and source have the same storage",
		},
	}

	_, client := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.ReplicateRepository(ctx, tc.input)
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestReplicateRepository_BadRepository(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testReplicateRepositoryBadRepository)
}

func testReplicateRepositoryBadRepository(t *testing.T, ctx context.Context) {
	for _, tc := range []struct {
		desc          string
		invalidSource bool
		invalidTarget bool
		error         func(testing.TB, error)
	}{
		{
			desc:          "target invalid",
			invalidTarget: true,
		},
		{
			desc:          "source invalid",
			invalidSource: true,
			error: func(t testing.TB, actual error) {
				testhelper.RequireGrpcError(t, actual, helper.ErrNotFoundf("source repository does not exist"))
			},
		},
		{
			desc:          "both invalid",
			invalidSource: true,
			invalidTarget: true,
			error: func(t testing.TB, actual error) {
				require.Equal(t, ErrInvalidSourceRepository, actual)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "target"))
			cfg := cfgBuilder.Build(t)

			testcfg.BuildGitalyHooks(t, cfg)
			testcfg.BuildGitalySSH(t, cfg)

			serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
			cfg.SocketPath = serverSocketPath

			client := newRepositoryClient(t, cfg, serverSocketPath)

			sourceRepo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
			targetRepo, targetRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[1], gittest.CloneRepoOpts{
				RelativePath: sourceRepo.RelativePath,
			})

			var invalidRepos []*gitalypb.Repository
			if tc.invalidSource {
				invalidRepos = append(invalidRepos, sourceRepo)
			}
			if tc.invalidTarget {
				invalidRepos = append(invalidRepos, targetRepo)
			}

			locator := config.NewLocator(cfg)
			for _, invalidRepo := range invalidRepos {
				invalidRepoPath, err := locator.GetPath(invalidRepo)
				require.NoError(t, err)

				// delete git data so make the repo invalid
				for _, path := range []string{"refs", "objects", "HEAD"} {
					require.NoError(t, os.RemoveAll(filepath.Join(invalidRepoPath, path)))
				}
			}

			ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

			_, err := client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
				Repository: targetRepo,
				Source:     sourceRepo,
			})
			if tc.error != nil {
				tc.error(t, err)
				return
			}

			require.NoError(t, err)
			gittest.Exec(t, cfg, "-C", targetRepoPath, "fsck")
		})
	}
}

func TestReplicateRepository_FailedFetchInternalRemote(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testReplicateRepositoryFailedFetchInternalRemote)
}

func testReplicateRepositoryFailedFetchInternalRemote(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t, testcfg.WithStorages("default", "replica"))
	testcfg.BuildGitalyHooks(t, cfg)
	testcfg.BuildGitalySSH(t, cfg)

	// Our test setup does not allow for Praefects with multiple storages. We thus have to
	// disable Praefect here.
	cfg.SocketPath = runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())

	targetRepo, _ := gittest.InitRepo(t, cfg, cfg.Storages[1])

	// The source repository must be at the same path as the target repository, and it must be a
	// real repository. In order to still have the fetch fail, we corrupt the repository by
	// writing garbage into HEAD.
	sourceRepo := &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: targetRepo.RelativePath,
	}
	sourceRepoPath, err := config.NewLocator(cfg).GetPath(sourceRepo)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(sourceRepoPath, 0o777))
	gittest.Exec(t, cfg, "init", "--bare", sourceRepoPath)
	require.NoError(t, os.WriteFile(filepath.Join(sourceRepoPath, "HEAD"), []byte("garbage"), 0o666))

	ctx = testhelper.MergeOutgoingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	repoClient := newRepositoryClient(t, cfg, cfg.SocketPath)

	_, err = repoClient.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch: exit status 128")
}

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
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testFetchInternalRemoteSuccessful)
}

func testFetchInternalRemoteSuccessful(t *testing.T, ctx context.Context) {
	remoteCfg, remoteRepo, remoteRepoPath := testcfg.BuildWithRepo(t)
	testcfg.BuildGitalyHooks(t, remoteCfg)
	gittest.WriteCommit(t, remoteCfg, remoteRepoPath, gittest.WithBranch("master"))

	remoteAddr := testserver.RunGitalyServer(t, remoteCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
	}, testserver.WithDisablePraefect())

	localCfg, localRepoProto, localRepoPath := testcfg.BuildWithRepo(t)
	localRepo := localrepo.NewTestRepo(t, localCfg, localRepoProto)
	testcfg.BuildGitalySSH(t, localCfg)
	testcfg.BuildGitalyHooks(t, localCfg)
	gittest.Exec(t, remoteCfg, "-C", localRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature")

	referenceTransactionHookCalled := 0

	// We do not require the server's address, but it needs to be around regardless such that
	// `FetchInternalRemote` can reach the hook service which is injected via the config.
	testserver.RunGitalyServer(t, localCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
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
	assert.NoError(t, fetchInternalRemote(ctx, connsPool, localRepo, remoteRepo))

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
	testhelper.NewFeatureSets(featureflag.FetchInternalWithSidechannel).Run(t, testFetchInternalRemoteFailure)
}

func testFetchInternalRemoteFailure(t *testing.T, ctx context.Context) {
	cfg, repoProto, _ := testcfg.BuildWithRepo(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	ctx = testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

	connsPool := client.NewPool()
	defer connsPool.Close()

	err := fetchInternalRemote(ctx, connsPool, repo, &gitalypb.Repository{
		StorageName:  repoProto.GetStorageName(),
		RelativePath: "does-not-exist.git",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fatal: Could not read from remote repository")
}
