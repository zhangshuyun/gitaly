package repository

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestReplicateRepository(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil, testserver.WithDisablePraefect())
	cfg.SocketPath = serverSocketPath

	client := newRepositoryClient(t, cfg, serverSocketPath)

	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "source")
	t.Cleanup(cleanup)

	// create a loose object to ensure snapshot replication is used
	blobData, err := text.RandomHex(10)
	require.NoError(t, err)
	blobID := text.ChompBytes(testhelper.MustRunCommand(t, bytes.NewBuffer([]byte(blobData)), "git", "-C", repoPath, "hash-object", "-w", "--stdin"))

	// write info attributes
	attrFilePath := filepath.Join(repoPath, "info", "attributes")
	attrData := []byte("*.pbxproj binary\n")
	require.NoError(t, ioutil.WriteFile(attrFilePath, attrData, 0644))

	targetRepo := *repo
	targetRepo.StorageName = cfg.Storages[1].Name

	ctx, cancel := testhelper.Context()
	defer cancel()
	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = client.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     repo,
	})
	require.NoError(t, err)

	targetRepoPath := filepath.Join(cfg.Storages[1].Path, targetRepo.GetRelativePath())
	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "fsck")

	replicatedAttrFilePath := filepath.Join(targetRepoPath, "info", "attributes")
	replicatedAttrData, err := ioutil.ReadFile(replicatedAttrFilePath)
	require.NoError(t, err)
	require.Equal(t, string(attrData), string(replicatedAttrData), "info/attributes files must match")

	// create another branch
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))
	_, err = client.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     repo,
	})
	require.NoError(t, err)
	require.Equal(t,
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "--hash", "--verify", "refs/heads/branch"),
		testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "show-ref", "--hash", "--verify", "refs/heads/branch"),
	)

	// if an unreachable object has been replicated, that means snapshot replication was used
	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "cat-file", "-p", blobID)
}

func TestReplicateRepositoryInvalidArguments(t *testing.T) {
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

			sourceRepo, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "source")
			t.Cleanup(cleanup)

			targetRepo, targetRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[1], sourceRepo.RelativePath)
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
			testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "fsck")
		})
	}
}

func TestReplicateRepository_FailedFetchInternalRemote(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("default", "replica"))
	cfg := cfgBuilder.Build(t)

	cfg.SocketPath = runServerWithBadFetchInternalRemote(t, cfg)

	locator := config.NewLocator(cfg)

	testRepo, _, cleanupRepo := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanupRepo)

	repoClient := newRepositoryClient(t, cfg, cfg.SocketPath)

	targetRepo := *testRepo
	targetRepo.StorageName = cfg.Storages[1].Name

	targetRepoPath, err := locator.GetPath(&targetRepo)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(targetRepoPath, 0755))
	testhelper.MustRunCommand(t, nil, "touch", filepath.Join(targetRepoPath, "invalid_git_repo"))

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.Error(t, err)
}

func runServerWithBadFetchInternalRemote(t *testing.T, cfg config.Cfg) string {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	internalListener, err := net.Listen("unix", cfg.GitalyInternalSocketPath())
	require.NoError(t, err)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	gitalypb.RegisterRepositoryServiceServer(server, NewServer(
		cfg,
		nil,
		config.NewLocator(cfg),
		transaction.NewManager(cfg, backchannel.NewRegistry()),
		gitCmdFactory,
		catfile.NewCache(gitCmdFactory, cfg),
	))
	gitalypb.RegisterRemoteServiceServer(server, &mockRemoteServer{})
	reflection.Register(server)

	go server.Serve(listener)
	go server.Serve(internalListener)
	t.Cleanup(server.Stop)
	return "unix://" + serverSocketPath
}

type mockRemoteServer struct {
	gitalypb.UnimplementedRemoteServiceServer
}

func (m *mockRemoteServer) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	return &gitalypb.FetchInternalRemoteResponse{
		Result: false,
	}, nil
}
