package repository_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestReplicateRepository(t *testing.T) {
	tmpPath, cleanup := testhelper.TempDir(t)
	defer cleanup()

	replicaPath := filepath.Join(tmpPath, "replica")
	require.NoError(t, os.MkdirAll(replicaPath, 0755))

	defer func(storages []config.Storage) {
		config.Config.Storages = storages
	}(config.Config.Storages)

	config.Config.Storages = []config.Storage{
		config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
		config.Storage{
			Name: "replica",
			Path: replicaPath,
		},
	}

	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	testRepo, testRepoPath, cleanupRepo := gittest.CloneRepo(t)
	defer cleanupRepo()

	// create a loose object to ensure snapshot replication is used
	blobData, err := text.RandomHex(10)
	require.NoError(t, err)
	blobID := text.ChompBytes(testhelper.MustRunCommand(t, bytes.NewBuffer([]byte(blobData)), "git", "-C", testRepoPath, "hash-object", "-w", "--stdin"))

	config.Config.SocketPath = serverSocketPath

	repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	// write info attributes
	attrFilePath := filepath.Join(testRepoPath, "info", "attributes")
	attrData := []byte("*.pbxproj binary\n")
	require.NoError(t, ioutil.WriteFile(attrFilePath, attrData, 0644))

	targetRepo := *testRepo
	targetRepo.StorageName = "replica"

	ctx, cancel := testhelper.Context()
	defer cancel()
	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.NoError(t, err)

	targetRepoPath, err := locator.GetRepoPath(&targetRepo)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "fsck")

	replicatedAttrFilePath := filepath.Join(targetRepoPath, "info", "attributes")
	replicatedAttrData, err := ioutil.ReadFile(replicatedAttrFilePath)
	require.NoError(t, err)
	require.Equal(t, string(attrData), string(replicatedAttrData), "info/attributes files must match")

	// create another branch
	_, anotherNewBranch := gittest.CreateCommitOnNewBranch(t, testRepoPath)
	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.NoError(t, err)
	require.Equal(t,
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show-ref", "--hash", "--verify", fmt.Sprintf("refs/heads/%s", anotherNewBranch)),
		testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "show-ref", "--hash", "--verify", fmt.Sprintf("refs/heads/%s", anotherNewBranch)),
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

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := repository.RunRepoServer(t, locator)
	defer stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

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
				require.Equal(t, repository.ErrInvalidSourceRepository, actual)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			targetStorageRoot, cleanup := testhelper.TempDir(t)
			defer cleanup()

			defer func(storages []config.Storage) {
				config.Config.Storages = storages
			}(config.Config.Storages)

			config.Config.Storages = []config.Storage{
				config.Storage{
					Name: "default",
					Path: testhelper.GitlabTestStoragePath(),
				},
				config.Storage{
					Name: "target",
					Path: targetStorageRoot,
				},
			}

			sourceRepo, _, cleanupRepo := gittest.CloneRepo(t)
			defer cleanupRepo()

			targetRepo := gittest.CloneRepoAtStorageRoot(t, targetStorageRoot, sourceRepo.RelativePath)
			targetRepo.StorageName = "target"

			var invalidRepos []*gitalypb.Repository
			if tc.invalidSource {
				invalidRepos = append(invalidRepos, sourceRepo)
			}
			if tc.invalidTarget {
				invalidRepos = append(invalidRepos, targetRepo)
			}

			locator := config.NewLocator(config.Config)
			for _, invalidRepo := range invalidRepos {
				invalidRepoPath, err := locator.GetPath(invalidRepo)
				require.NoError(t, err)

				// delete git data so make the repo invalid
				for _, path := range []string{"refs", "objects", "HEAD"} {
					require.NoError(t, os.RemoveAll(filepath.Join(invalidRepoPath, path)))
				}
			}

			serverSocketPath, clean := runFullServer(t)
			defer clean()

			config.Config.SocketPath = serverSocketPath

			repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			injectedCtx := metadata.NewOutgoingContext(ctx, md)

			_, err := repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
				Repository: targetRepo,
				Source:     sourceRepo,
			})
			if tc.error != nil {
				tc.error(t, err)
				return
			}

			require.NoError(t, err)
			testhelper.MustRunCommand(t, nil, "git", "-C", filepath.Join(targetStorageRoot, targetRepo.RelativePath), "fsck")
		})
	}
}

func TestReplicateRepository_FailedFetchInternalRemote(t *testing.T) {
	tmpPath, cleanup := testhelper.TempDir(t)
	defer cleanup()

	replicaPath := filepath.Join(tmpPath, "replica")
	require.NoError(t, os.MkdirAll(replicaPath, 0755))

	defer func(storages []config.Storage) {
		config.Config.Storages = storages
	}(config.Config.Storages)

	config.Config.Storages = []config.Storage{
		config.Storage{
			Name: "default",
			Path: testhelper.GitlabTestStoragePath(),
		},
		config.Storage{
			Name: "replica",
			Path: replicaPath,
		},
	}

	server, serverSocketPath := runServerWithBadFetchInternalRemote(t)
	defer server.Stop()

	locator := config.NewLocator(config.Config)

	testRepo, _, cleanupRepo := gittest.CloneRepo(t)
	defer cleanupRepo()

	config.Config.SocketPath = serverSocketPath

	repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	targetRepo := *testRepo
	targetRepo.StorageName = "replica"

	targetRepoPath, err := locator.GetPath(&targetRepo)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(targetRepoPath, 0755))
	testhelper.MustRunCommand(t, nil, "touch", filepath.Join(targetRepoPath, "invalid_git_repo"))

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.Error(t, err)
}

func runServerWithBadFetchInternalRemote(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	internalListener, err := net.Listen("unix", config.Config.GitalyInternalSocketPath())
	require.NoError(t, err)

	gitalypb.RegisterRepositoryServiceServer(server, repository.NewServer(config.Config, repository.RubyServer, config.NewLocator(config.Config), transaction.NewManager(config.Config, backchannel.NewRegistry()), git.NewExecCommandFactory(config.Config)))
	gitalypb.RegisterRemoteServiceServer(server, &mockRemoteServer{})
	reflection.Register(server)

	go server.Serve(listener)
	go server.Serve(internalListener)

	return server, "unix://" + serverSocketPath
}

type mockRemoteServer struct {
	gitalypb.UnimplementedRemoteServiceServer
}

func (m *mockRemoteServer) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	return &gitalypb.FetchInternalRemoteResponse{
		Result: false,
	}, nil
}
