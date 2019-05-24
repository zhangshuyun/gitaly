package cleanup_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"google.golang.org/grpc/codes"

	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestApplyBfgObjectMapSuccess(t *testing.T) {
	server, serverSocketPath := runCleanupServiceServer(t)
	defer server.Stop()

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	headCommit, err := log.GetCommit(ctx, testRepo, "HEAD")
	require.NoError(t, err)

	// Create some refs pointing to HEAD
	for _, ref := range []string{
		"refs/environments/1", "refs/keep-around/1", "refs/merge-requests/1",
		"refs/heads/_keep", "refs/tags/_keep", "refs/notes/_keep",
	} {
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "update-ref", ref, headCommit.Id)
	}

	objectMapData := fmt.Sprintf("%s %s\n", headCommit.Id, strings.Repeat("0", 40))
	require.NoError(t, doRequest(ctx, t, testRepo, client, objectMapData))

	// Ensure that the internal refs are gone, but the others still exist
	refs := testhelper.GetRepositoryRefs(t, testRepoPath)
	assert.NotContains(t, refs, "refs/environments/1")
	assert.NotContains(t, refs, "refs/keep-around/1")
	assert.NotContains(t, refs, "refs/merge-requests/1")
	assert.Contains(t, refs, "refs/heads/_keep")
	assert.Contains(t, refs, "refs/tags/_keep")
	assert.Contains(t, refs, "refs/notes/_keep")
}

func TestApplyBfgObjectMapFailsOnInvalidInput(t *testing.T) {
	server, serverSocketPath := runCleanupServiceServer(t)
	defer server.Stop()

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	err := doRequest(ctx, t, testRepo, client, "invalid-data here as you can see")
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func doRequest(ctx context.Context, t *testing.T, repo *gitalypb.Repository, client gitalypb.CleanupServiceClient, objectMap string) error {
	// Split the data across multiple requests
	parts := strings.SplitN(objectMap, " ", 2)
	req1 := &gitalypb.ApplyBfgObjectMapRequest{
		Repository: repo,
		ObjectMap:  []byte(parts[0] + " "),
	}
	req2 := &gitalypb.ApplyBfgObjectMapRequest{ObjectMap: []byte(parts[1])}

	stream, err := client.ApplyBfgObjectMap(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(req1))
	require.NoError(t, stream.Send(req2))

	_, err = stream.CloseAndRecv()
	return err
}
