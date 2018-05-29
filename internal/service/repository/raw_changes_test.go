package repository

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestGetRawChanges(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		oldRev string
		newRev string
		size   int
	}{
		{
			oldRev: "55bc176024cfa3baaceb71db584c7e5df900ea65",
			newRev: "7975be0116940bf2ad4321f79d02a55c5f7779aa",
			size:   2,
		},
		{
			oldRev: "0000000000000000000000000000000000000000",
			newRev: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			size:   3,
		},
	}

	for _, tc := range testCases {
		ctx, cancel := testhelper.Context()
		defer cancel()

		req := &pb.GetRawChangesRequest{testRepo, tc.oldRev, tc.newRev}

		resp, err := client.GetRawChanges(ctx, req)
		require.NoError(t, err)

		msg, err := resp.Recv()
		require.NoError(t, err)

		changes := msg.GetRawChanges()
		require.Len(t, changes, tc.size)
	}
}

func TestGetRawChangesFailures(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		oldRev string
		newRev string
		code   codes.Code
	}{
		{
			oldRev: "",
			newRev: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:   codes.InvalidArgument,
		},
		{
			// A Gitaly commit, unresolvable in gitlab-test
			oldRev: "32800ed8206c0087f65e90a1a396b76d3c33f648",
			newRev: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:   codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		ctx, cancel := testhelper.Context()
		defer cancel()

		req := &pb.GetRawChangesRequest{testRepo, tc.oldRev, tc.newRev}

		resp, err := client.GetRawChanges(ctx, req)
		require.NoError(t, err)

		_, err = resp.Recv()
		testhelper.RequireGrpcError(t, err, tc.code)
	}
}

func TestGetRawChangesManyFiles(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	initCommit := "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"
	req := &pb.GetRawChangesRequest{testRepo, initCommit, "many_files"}

	c, err := client.GetRawChanges(ctx, req)
	require.NoError(t, err)

	changes := []*pb.GetRawChangesResponse_RawChange{}
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		changes = append(changes, resp.GetRawChanges()...)
	}

	require.True(t, len(changes) >= 1041, "Changes has to contain at least 1041 changes")
}

func TestGetRawChangesMappingOperations(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &pb.GetRawChangesRequest{
		testRepo,
		"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
		"94bb47ca1297b7b3731ff2a36923640991e9236f",
	}

	c, err := client.GetRawChanges(ctx, req)
	require.NoError(t, err)
	msg, err := c.Recv()
	require.NoError(t, err)

	ops := []pb.GetRawChangesResponse_RawChange_Operation{}
	for _, change := range msg.GetRawChanges() {
		ops = append(ops, change.GetOperation())
	}

	expected := []pb.GetRawChangesResponse_RawChange_Operation{
		pb.GetRawChangesResponse_RawChange_RENAMED,
		pb.GetRawChangesResponse_RawChange_MODIFIED,
		pb.GetRawChangesResponse_RawChange_ADDED,
	}

	firstChange := &pb.GetRawChangesResponse_RawChange{
		BlobId:    "53855584db773c3df5b5f61f72974cb298822fbb",
		Size:      22846,
		NewPath:   "CHANGELOG.md",
		OldPath:   "CHANGELOG",
		Operation: pb.GetRawChangesResponse_RawChange_RENAMED,
	}

	require.Equal(t, firstChange, msg.GetRawChanges()[0])
	require.EqualValues(t, expected, ops)
}
