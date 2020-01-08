package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	testhelper.AssertPathNotExists(t, testRepoPath)
}

func TestRemoveRepositoryDoesNotExist(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "/does/not/exist"}})
	require.NoError(t, err)
}
