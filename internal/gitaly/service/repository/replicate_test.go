package repository

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestReplicateRepository(t *testing.T) {
	t.Parallel()
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	client := newRepositoryClient(t, cfg, serverSocketPath)

	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
	t.Cleanup(cleanup)

	// create a loose object to ensure snapshot replication is used
	blobData, err := text.RandomHex(10)
	require.NoError(t, err)
	blobID := text.ChompBytes(gittest.ExecStream(t, cfg, bytes.NewBuffer([]byte(blobData)), "-C", repoPath, "hash-object", "-w", "--stdin"))

	// write info attributes
	attrFilePath := filepath.Join(repoPath, "info", "attributes")
	attrData := []byte("*.pbxproj binary\n")
	require.NoError(t, ioutil.WriteFile(attrFilePath, attrData, 0644))

	// Write a modified gitconfig
	gittest.Exec(t, cfg, "-C", repoPath, "config", "please.replicate", "me")
	configData := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.Contains(t, string(configData), "[please]\n\treplicate = me\n")

	targetRepo := proto.Clone(repo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	ctx, cancel := testhelper.Context()
	defer cancel()
	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = client.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
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
	_, err = client.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
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

func TestReplicateRepository_transactional(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReplicateRepositoryDirectFetch,
	}).Run(t, testReplicateRepositoryTransactional)
}

func testReplicateRepositoryTransactional(t *testing.T, ctx context.Context) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	sourceRepo, sourceRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
	t.Cleanup(cleanup)

	targetRepo := proto.Clone(sourceRepo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	votes := 0
	txServer := testTransactionServer{
		vote: func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
			votes++
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_COMMIT,
			}, nil
		},
	}

	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = helper.IncomingToOutgoing(ctx)
	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	client := newMuxedRepositoryClient(t, ctx, cfg, serverSocketPath, backchannel.NewClientHandshaker(
		testhelper.DiscardTestEntry(t),
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
	require.Equal(t, 1, votes)

	// We're now changing a reference in the source repository such that we can observe changes
	// in the target repo.
	gittest.Exec(t, cfg, "-C", sourceRepoPath, "update-ref", "refs/heads/master", "refs/heads/master~")

	votes = 0

	// And the second invocation uses FetchInternalRemote.
	_, err = client.ReplicateRepository(ctx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     sourceRepo,
	})

	if featureflag.IsEnabled(ctx, featureflag.ReplicateRepositoryDirectFetch) {
		require.NoError(t, err)
		require.Equal(t, 2, votes)
	} else {
		// This is failing because we do a nested mutating RPC in `ReplicateRepository()` to
		// `FetchInternalRemote()`. Because we simply pass along the incoming context as an
		// outgoing one, the server would try to vote on the backchannel. But given that the
		// connection is not to Praefect but to Gitaly now, it's trying to cast votes on a
		// non-multiplexed Gitaly connection instead of against the expected Praefect peer.
		require.Error(t, err)
		require.Contains(t, err.Error(), "ref updates aborted by hook")
		require.Equal(t, 0, votes)
	}
}

func TestReplicateRepositoryInvalidArguments(t *testing.T) {
	t.Parallel()
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

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.ReplicateRepository(ctx, tc.input)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestReplicateRepository_BadRepository(t *testing.T) {
	t.Parallel()
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
				testhelper.RequireGrpcError(t, actual, codes.NotFound)
				require.Contains(t, actual.Error(), "rpc error: code = NotFound desc = GetRepoPath: not a git repository:")
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

			testhelper.ConfigureGitalyHooksBin(t, cfg)
			testhelper.ConfigureGitalySSHBin(t, cfg)

			serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
			cfg.SocketPath = serverSocketPath

			client := newRepositoryClient(t, cfg, serverSocketPath)

			sourceRepo, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
			t.Cleanup(cleanup)

			targetRepo, targetRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[1], sourceRepo.RelativePath)
			t.Cleanup(cleanup)

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

			ctx, cancel := testhelper.Context()
			defer cancel()

			md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
			injectedCtx := metadata.NewOutgoingContext(ctx, md)

			_, err := client.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
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
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	cfg.SocketPath = runServerWithBadFetchInternalRemote(t, cfg)

	locator := config.NewLocator(cfg)

	testRepo, _, cleanupRepo := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanupRepo)

	repoClient := newRepositoryClient(t, cfg, cfg.SocketPath)

	targetRepo := proto.Clone(testRepo).(*gitalypb.Repository)
	targetRepo.StorageName = cfg.Storages[1].Name

	targetRepoPath, err := locator.GetPath(targetRepo)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(targetRepoPath, 0755))
	testhelper.MustRunCommand(t, nil, "touch", filepath.Join(targetRepoPath, "invalid_git_repo"))

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: targetRepo,
		Source:     testRepo,
	})
	require.Error(t, err)
}

func runServerWithBadFetchInternalRemote(t *testing.T, cfg config.Cfg) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterRemoteServiceServer(srv, &mockRemoteServer{})
	})
}

type mockRemoteServer struct {
	gitalypb.UnimplementedRemoteServiceServer
}

func (m *mockRemoteServer) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	return &gitalypb.FetchInternalRemoteResponse{
		Result: false,
	}, nil
}
