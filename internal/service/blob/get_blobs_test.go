package blob

import (
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulGetBlobsRequest(t *testing.T) {
	server, serverSocketPath := runBlobServer(t)
	defer server.Stop()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	expectedBlobs := []*gitalypb.GetBlobsResponse{
		{
			Path: []byte("CHANGELOG"),
			Size: 22846,
			Oid:  "53855584db773c3df5b5f61f72974cb298822fbb",
			Mode: 0100644,
		},
		{
			Path: []byte("files/lfs/lfs_object.iso"),
			Size: 133,
			Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
			Mode: 0100644,
		},
		{
			Path: []byte("files/big-lorem.txt"),
			Size: 30602785,
			Oid:  "c9d591740caed845a78ed529fadb3fb96c920cb2",
			Mode: 0100644,
		},
		{
			Path:        []byte("six"),
			Size:        0,
			Oid:         "409f37c4f05865e4fb208c771485f211a22c4c2d",
			Mode:        0160000,
			IsSubmodule: true,
		},
	}
	revision := "ef16b8d2b204706bd8dc211d4011a5bffb6fc0c2"
	limits := []int{-1, 0, 10 * 1024 * 1024}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "worktree", "add", "blobs-sandbox", revision)

	var revisionPaths []*gitalypb.GetBlobsRequest_RevisionPath
	for _, blob := range expectedBlobs {
		revisionPaths = append(revisionPaths, &gitalypb.GetBlobsRequest_RevisionPath{Revision: revision, Path: blob.Path})
	}
	revisionPaths = append(revisionPaths,
		&gitalypb.GetBlobsRequest_RevisionPath{Revision: "does-not-exist", Path: []byte("CHANGELOG")},
		&gitalypb.GetBlobsRequest_RevisionPath{Revision: revision, Path: []byte("file-that-does-not-exist")},
	)

	for _, limit := range limits {
		t.Run(fmt.Sprintf("limit = %d", limit), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.GetBlobsRequest{
				Repository:    testRepo,
				RevisionPaths: revisionPaths,
				Limit:         int64(limit),
			}

			c, err := client.GetBlobs(ctx, request)
			require.NoError(t, err)

			var receivedBlobs []*gitalypb.GetBlobsResponse
			var nonExistentBlobs []*gitalypb.GetBlobsResponse

			for {
				response, err := c.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				if len(response.Oid) == 0 && len(response.Data) == 0 {
					nonExistentBlobs = append(nonExistentBlobs, response)
				} else if len(response.Oid) != 0 {
					receivedBlobs = append(receivedBlobs, response)
				} else {
					require.NotEmpty(t, receivedBlobs)
					currentBlob := receivedBlobs[len(receivedBlobs)-1]
					currentBlob.Data = append(currentBlob.Data, response.Data...)
				}
			}

			require.Equal(t, 2, len(nonExistentBlobs))
			require.Equal(t, len(expectedBlobs), len(receivedBlobs))

			for i, receviedBlob := range receivedBlobs {
				expectedBlob := expectedBlobs[i]
				expectedBlob.Revision = revision
				if !expectedBlob.IsSubmodule {
					expectedBlob.Data = testhelper.MustReadFile(t, path.Join(testRepoPath, "blobs-sandbox", string(expectedBlob.Path)))
				}
				if limit == 0 {
					expectedBlob.Data = nil
				}
				if limit > 0 && limit < len(expectedBlob.Data) {
					expectedBlob.Data = expectedBlob.Data[:limit]
				}

				require.Equal(t, expectedBlob, receviedBlob)
			}
		})
	}
}

func TestFailedGetBlobsRequestDueToValidation(t *testing.T) {
	server, serverSocketPath := runBlobServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *gitalypb.GetBlobsRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.GetBlobsRequest{
				RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
					{Revision: "does-not-exist", Path: []byte("file")},
				},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty RevisionPaths",
			request: &gitalypb.GetBlobsRequest{
				Repository: testRepo,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "invalid Revision",
			request: &gitalypb.GetBlobsRequest{
				Repository: testRepo,
				RevisionPaths: []*gitalypb.GetBlobsRequest_RevisionPath{
					{
						Path:     []byte("CHANGELOG"),
						Revision: "--output=/meow",
					},
				},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetBlobs(ctx, testCase.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
