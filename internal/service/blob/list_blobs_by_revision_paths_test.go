package blob

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
)

func TestListBlobsByRevisionPath(t *testing.T) {
	server, serverSocketPath := runBlobServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		path     string
		oid      string
		limit    int64
		size     int64
		blobOid  string
		notFound bool
	}{
		{
			path: "",
			oid:  "e63f41fe459e62e1228fcef60d7189127aeba95a",
		},
		{
			path:    "CHANGELOG",
			oid:     "e63f41fe459e62e1228fcef60d7189127aeba95a",
			blobOid: "53855584db773c3df5b5f61f72974cb298822fbb",
			limit:   1,
		},
		{
			path:    "CHANGELOG",
			oid:     "e63f41fe459e62e1228fcef60d7189127aeba95a",
			blobOid: "53855584db773c3df5b5f61f72974cb298822fbb",
			limit:   -1,
		},
		{
			path:    "bar/branch-test.txt",
			oid:     "e63f41fe459e62e1228fcef60d7189127aeba95a",
			blobOid: "93e123ac8a3e6a0b600953d7598af629dec7b735",
			limit:   100,
		},
		{
			path:  "bar/branch-test.txt",
			oid:   "",
			limit: 100,
		},
	}

	var revPaths []*pb.ListBlobsByRevisionPathRequest_RevisionPath
	for _, tc := range testCases {
		revPath := pb.ListBlobsByRevisionPathRequest_RevisionPath{
			CommitOid: tc.oid,
			Path:      tc.path,
			Limit:     tc.limit,
		}

		revPaths = append(revPaths, &revPath)
	}

	request := &pb.ListBlobsByRevisionPathRequest{
		Repository:    testRepo,
		RevisionPaths: revPaths,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.ListBlobsByRevisionPath(ctx, request)
	require.NoError(t, err)

	var blobs []*pb.ListBlobsByRevisionPathResponse
	for {
		blob, err := stream.Recv()
		if err != io.EOF {
			break
		}

		require.NoError(t, err)
		blobs = append(blobs, blob)
	}

	// It excludes unfound examples
	assert.Len(t, blobs, 3)

	// Cross check
	for _, blob := range blobs {
		resp, err := client.GetBlob(ctx, &pb.GetBlobRequest{testRepo, blob.Oid, blob.Size})
		require.NoError(t, err)

		msg, err := resp.Recv()
		require.NoError(t, err)

		assert.Equal(t, msg.Data, blob.Data)
		assert.Equal(t, msg.Size, blob.Size)
		assert.Equal(t, msg.Oid, blob.Oid)
	}
}

func TestLookupRevision(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		path    string
		oid     string
		blobOid string
	}{
		{
			path:    "CHANGELOG",
			oid:     "e63f41fe459e62e1228fcef60d7189127aeba95a",
			blobOid: "53855584db773c3df5b5f61f72974cb298822fbb",
		},
		{
			path:    "bar/branch-test.txt",
			oid:     "e63f41fe459e62e1228fcef60d7189127aeba95a",
			blobOid: "93e123ac8a3e6a0b600953d7598af629dec7b735",
		},
	}

	for _, tc := range testCases {
		// Context not needed here, so passing in a mock
		oid, err := lookupRevision(context.TODO(), testRepo, fmt.Sprintf("%s:%s", tc.oid, tc.path))
		assert.NoError(t, err)
		assert.Equal(t, tc.blobOid, oid)
	}
}

func TestReadLimit(t *testing.T) {
	testCase := []struct {
		input  int64
		output int64
	}{
		{1, 1},
		{-1, BatchBlobMaxReadLimit},
		{0, BatchBlobMaxReadLimit},
		{1 << 21, BatchBlobMaxReadLimit},
	}

	for _, tc := range testCase {
		assert.Equal(t, tc.output, readLimit(tc.input))
	}
}
