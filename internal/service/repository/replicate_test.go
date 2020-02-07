package repository_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestReplicateRepository(t *testing.T) {
	tmpPath, cleanup := testhelper.TempDir(t, t.Name())
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

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	testRepo, testRepoPath, cleanupRepo := testhelper.NewTestRepo(t)
	defer cleanupRepo()

	// create a loose object to ensure snapshot replication is used
	blobData, err := text.RandomHex(10)
	require.NoError(t, err)
	blobID := text.ChompBytes(testhelper.MustRunCommand(t, bytes.NewBuffer([]byte(blobData)), "git", "-C", testRepoPath, "hash-object", "-w", "--stdin"))

	config.Config.SocketPath = serverSocketPath

	repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	// write info attributes
	attrFilePath := path.Join(testRepoPath, "info", "attributes")
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

	targetRepoPath, err := helper.GetRepoPath(&targetRepo)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "fsck")

	replicatedAttrFilePath := path.Join(targetRepoPath, "info", "attributes")
	replicatedAttrData, err := ioutil.ReadFile(replicatedAttrFilePath)
	require.NoError(t, err)
	require.Equal(t, string(attrData), string(replicatedAttrData), "info/attributes files must match")

	// create another branch
	_, anotherNewBranch := testhelper.CreateCommitOnNewBranch(t, testRepoPath)
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
	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "cat-file", "-p", string(blobID))
}

func TestReplicateRepositoryInvalidArguments(t *testing.T) {
	testCases := []struct {
		description   string
		input         *gitalypb.ReplicateRepositoryRequest
		expectedError string
	}{
		{
			description: "everything ✅",
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

	server, serverSocketPath := repository.RunRepoServer(t)
	defer server.Stop()

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
	tmpPath, cleanup := testhelper.TempDir(t, t.Name())
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

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	testRepo, _, cleanupRepo := testhelper.NewTestRepo(t)
	defer cleanupRepo()

	config.Config.SocketPath = serverSocketPath

	repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	targetRepo := *testRepo
	targetRepo.StorageName = "replica"

	targetRepoPath, err := helper.GetPath(&targetRepo)
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
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "fsck")
}

func TestReplicateRepositoryConcurrent(t *testing.T) {
	tmpPath, cleanup := testhelper.TempDir(t, t.Name())
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

	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	testRepo, _, cleanupRepo := testhelper.NewTestRepo(t)
	defer cleanupRepo()

	config.Config.SocketPath = serverSocketPath

	repoClient, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	targetRepo := *testRepo
	targetRepo.StorageName = "replica"

	targetRepoPath, err := helper.GetPath(&targetRepo)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(targetRepoPath, 0755))

	lockfilePath := filepath.Join(targetRepoPath, repository.ReplicationLockFileName)

	// write a lockfile in the repository directory
	f, err := os.OpenFile(lockfilePath, os.O_EXCL|os.O_CREATE, 0)
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()
	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	injectedCtx := metadata.NewOutgoingContext(ctx, md)

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.NoError(t, err)

	require.False(t, helper.IsGitDirectory(targetRepoPath), "replication should not have happened")

	// remove lockfile
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove(lockfilePath))

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.NoError(t, err)
	require.True(t, helper.IsGitDirectory(targetRepoPath), "replication should have happened")
}
