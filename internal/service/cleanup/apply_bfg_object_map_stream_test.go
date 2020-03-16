package cleanup

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestApplyBfgObjectMapStreamSuccess(t *testing.T) {
	serverSocketPath, stop := runCleanupServiceServer(t)
	defer stop()

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	headCommit, err := log.GetCommit(ctx, testRepo, "HEAD")
	require.NoError(t, err)

	// A known blob: the CHANGELOG in the test repository
	blobID := "53855584db773c3df5b5f61f72974cb298822fbb"

	// A known tag: v1.0.0
	tagID := "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8"

	// Create some refs pointing to HEAD
	for _, ref := range []string{
		"refs/environments/1", "refs/keep-around/1", "refs/merge-requests/1",
		"refs/heads/_keep", "refs/tags/_keep", "refs/notes/_keep",
	} {
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "update-ref", ref, headCommit.Id)
	}

	objectMapData := fmt.Sprintf(
		strings.Repeat("%s %s\n", 4),
		headCommit.Id, headCommit.Id,
		git.NullSHA, blobID,
		git.NullSHA, tagID,
		git.NullSHA, git.NullSHA,
	)

	entries, err := doStreamingRequest(ctx, t, testRepo, client, objectMapData)
	require.NoError(t, err)

	// Ensure that the internal refs are gone, but the others still exist
	refs := testhelper.GetRepositoryRefs(t, testRepoPath)
	assert.NotContains(t, refs, "refs/environments/1")
	assert.NotContains(t, refs, "refs/keep-around/1")
	assert.NotContains(t, refs, "refs/merge-requests/1")
	assert.Contains(t, refs, "refs/heads/_keep")
	assert.Contains(t, refs, "refs/tags/_keep")
	assert.Contains(t, refs, "refs/notes/_keep")

	// Ensure that the returned entry is correct
	require.Len(t, entries, 4, "wrong number of entries returned")
	requireEntry(t, entries[0], headCommit.Id, headCommit.Id, gitalypb.ObjectType_COMMIT)
	requireEntry(t, entries[1], git.NullSHA, blobID, gitalypb.ObjectType_BLOB)
	requireEntry(t, entries[2], git.NullSHA, tagID, gitalypb.ObjectType_TAG)
	requireEntry(t, entries[3], git.NullSHA, git.NullSHA, gitalypb.ObjectType_UNKNOWN)
}

func requireEntry(t *testing.T, entry *gitalypb.ApplyBfgObjectMapStreamResponse_Entry, oldOid, newOid string, objectType gitalypb.ObjectType) {
	require.Equal(t, objectType, entry.Type)
	require.Equal(t, oldOid, entry.OldOid)
	require.Equal(t, newOid, entry.NewOid)
}

func TestApplyBfgObjectMapStreamFailsOnInvalidInput(t *testing.T) {
	serverSocketPath, stop := runCleanupServiceServer(t)
	defer stop()

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	entries, err := doStreamingRequest(ctx, t, testRepo, client, "invalid-data here as you can see")
	require.Empty(t, entries)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func doStreamingRequest(ctx context.Context, t *testing.T, repo *gitalypb.Repository, client gitalypb.CleanupServiceClient, objectMap string) ([]*gitalypb.ApplyBfgObjectMapStreamResponse_Entry, error) {
	// Split the data across multiple requests
	parts := strings.SplitN(objectMap, " ", 2)
	req1 := &gitalypb.ApplyBfgObjectMapStreamRequest{
		Repository: repo,
		ObjectMap:  []byte(parts[0] + " "),
	}
	req2 := &gitalypb.ApplyBfgObjectMapStreamRequest{ObjectMap: []byte(parts[1])}

	server, err := client.ApplyBfgObjectMapStream(ctx)
	require.NoError(t, err)
	require.NoError(t, server.Send(req1))
	require.NoError(t, server.Send(req2))
	require.NoError(t, server.CloseSend())

	// receive all responses in a loop
	var entries []*gitalypb.ApplyBfgObjectMapStreamResponse_Entry
	for {
		rsp, err := server.Recv()
		if rsp != nil {
			entries = append(entries, rsp.GetEntries()...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return entries, nil
}
