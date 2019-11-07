package repository_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestReplicateRepository(t *testing.T) {
	tmpPath, cleanup := testhelper.TempDir(t, testhelper.GitlabTestStoragePath(), t.Name())
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

	config.Config.SocketPath = serverSocketPath

	testRepo, testRepoPath, cleanupRepo := testhelper.NewTestRepo(t)
	defer cleanupRepo()

	_, newBranch := testhelper.CreateCommitOnNewBranch(t, testRepoPath)

	// write info attributes
	attrFilePath := path.Join(testRepoPath, "info", "attributes")
	attrData := []byte("*.pbxproj binary\n")
	require.NoError(t, ioutil.WriteFile(attrFilePath, attrData, 0644))

	// create object pool
	pool, err := objectpool.NewObjectPool(testRepo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	poolCtx, cancel := testhelper.Context()

	require.NoError(t, pool.Create(poolCtx, testRepo))
	require.NoError(t, pool.Link(poolCtx, testRepo))

	cancel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(config.Config.Auth.Token)),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)

	repoClient := gitalypb.NewRepositoryServiceClient(conn)

	injectedCtx, err := helper.InjectGitalyServers(ctx, "default", serverSocketPath, config.Config.Auth.Token)
	require.NoError(t, err)

	targetRepo := *testRepo
	targetRepo.StorageName = "replica"

	_, err = repoClient.ReplicateRepository(injectedCtx, &gitalypb.ReplicateRepositoryRequest{
		Repository: &targetRepo,
		Source:     testRepo,
	})
	require.NoError(t, err)

	targetRepoPath, err := helper.GetRepoPath(&targetRepo)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "gc")

	require.True(t, getGitObjectDirSize(t, targetRepoPath) < 100, "expect a small object directory size")
	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "show-ref", "--verify", fmt.Sprintf("refs/heads/%s", newBranch))

	replicatedAttrFilePath := path.Join(targetRepoPath, "info", "attributes")
	replicatedAttrData, err := ioutil.ReadFile(replicatedAttrFilePath)
	require.NoError(t, err)
	require.Equal(t, attrData, replicatedAttrData, "info/attributes files must match")
}
