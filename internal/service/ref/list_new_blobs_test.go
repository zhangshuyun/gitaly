package ref

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListNewBlobs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	oid := "ab2c9622c02288a2bbaaf35d96088cfdff31d9d9"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-D", "gitaly-diff-stuff")

	testCases := []struct {
		revision     string
		blobs        []gitalypb.NewBlobObject
		responseCode codes.Code
	}{
		{
			revision: oid,
			blobs: []gitalypb.NewBlobObject{
				gitalypb.NewBlobObject{Oid: "389c7a36a6e133268b0d36b00e7ffc0f3a5b6651", Path: []byte("gitaly/file-with-pluses.txt"), Size: 20},
				gitalypb.NewBlobObject{Oid: "b1e67221afe8461efd244b487afca22d46b95eb8", Path: []byte("z-short-diff"), Size: 6},
			},
		},
		{
			revision:     "- rm -rf /",
			responseCode: codes.InvalidArgument,
		},
		{
			revision:     "1234deadbeef",
			responseCode: codes.InvalidArgument,
		},
		{
			revision: "7975be0116940bf2ad4321f79d02a55c5f7779aa",
		},
	}

	for _, tc := range testCases {
		request := &gitalypb.ListNewBlobsRequest{Repository: testRepo, CommitId: tc.revision, Limit: 0}

		stream, err := client.ListNewBlobs(ctx, request)
		require.NoError(t, err)

		var blobs []*gitalypb.NewBlobObject
		for {
			msg, err := stream.Recv()

			if err == io.EOF {
				break
			}
			if err != nil {
				require.Equal(t, tc.responseCode, status.Code(err))
				break
			}

			require.NoError(t, err)
			blobs = append(blobs, msg.NewBlobObjects...)
		}
		require.Len(t, blobs, len(tc.blobs))
		for i, blob := range blobs {
			require.Equal(t, blob.Oid, tc.blobs[i].Oid)
			require.Equal(t, blob.Path, tc.blobs[i].Path)
			require.Equal(t, blob.Size, tc.blobs[i].Size)
		}
	}
}
