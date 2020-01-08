package commit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type treeEntry struct {
	oid        string
	objectType gitalypb.TreeEntryResponse_ObjectType
	data       []byte
	mode       int32
	size       int64
}

func TestSuccessfulTreeEntry(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		revision          []byte
		path              []byte
		limit             int64
		expectedTreeEntry treeEntry
	}{
		{
			revision: []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
			path:     []byte("MAINTENANCE.md"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_BLOB,
				oid:        "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
				size:       1367,
				mode:       0100644,
				data:       testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
			},
		},
		{
			revision: []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
			path:     []byte("MAINTENANCE.md"),
			limit:    40 * 1024,
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_BLOB,
				oid:        "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
				size:       1367,
				mode:       0100644,
				data:       testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
			},
		},
		{
			revision: []byte("38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e"),
			path:     []byte("with space/README.md"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_BLOB,
				oid:        "8c3014aceae45386c3c026a7ea4a1f68660d51d6",
				size:       36,
				mode:       0100644,
				data:       testhelper.MustReadFile(t, "testdata/with-space-readme-md-blob.txt"),
			},
		},
		{
			revision: []byte("372ab6950519549b14d220271ee2322caa44d4eb"),
			path:     []byte("gitaly/file-with-multiple-chunks"),
			limit:    30 * 1024,
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_BLOB,
				oid:        "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
				size:       42220,
				mode:       0100644,
				data:       testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-truncated-blob.txt"),
			},
		},
		{
			revision:          []byte("deadfacedeadfacedeadfacedeadfacedeadface"),
			path:              []byte("with space/README.md"),
			expectedTreeEntry: treeEntry{},
		},
		{
			revision:          []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			path:              []byte("missing.rb"),
			expectedTreeEntry: treeEntry{},
		},
		{
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			path:     []byte("gitlab-grack"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_COMMIT,
				oid:        "645f6c4c82fd3f5e06f67134450a570b795e55a6",
				mode:       0160000,
			},
		},
		{
			revision: []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"),
			path:     []byte("files/js"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_TREE,
				oid:        "31405c5ddef582c5a9b7a85230413ff90e2fe720",
				size:       83,
				mode:       040000,
			},
		},
		{
			revision: []byte("c347ca2e140aa667b968e51ed0ffe055501fe4f4"),
			path:     []byte("files/js/"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_TREE,
				oid:        "31405c5ddef582c5a9b7a85230413ff90e2fe720",
				size:       83,
				mode:       040000,
			},
		},
		{
			revision: []byte("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
			path:     []byte("foo/bar/.gitkeep"),
			expectedTreeEntry: treeEntry{
				objectType: gitalypb.TreeEntryResponse_BLOB,
				oid:        "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
				size:       0,
				mode:       0100644,
			},
		},
		{
			revision:          []byte("913c66a37b4a45b9769037c55c2d238bd0942d2e"),
			path:              []byte("../bar/.gitkeep"), // Git blows up on paths like this
			expectedTreeEntry: treeEntry{},
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("test case: revision=%q path=%q", testCase.revision, testCase.path), func(t *testing.T) {
			request := &gitalypb.TreeEntryRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
				Path:       testCase.path,
				Limit:      testCase.limit,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.TreeEntry(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			assertExactReceivedTreeEntry(t, c, &testCase.expectedTreeEntry)
		})
	}
}

func TestFailedTreeEntryRequestDueToValidationError(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	revision := []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72")
	path := []byte("a/b/c")

	rpcRequests := []gitalypb.TreeEntryRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: revision, Path: path}, // Repository doesn't exist
		{Repository: nil, Revision: revision, Path: path},                                                             // Repository is nil
		{Repository: testRepo, Revision: nil, Path: path},                                                             // Revision is empty
		{Repository: testRepo, Revision: revision},                                                                    // Path is empty
		{Repository: testRepo, Revision: []byte("--output=/meow"), Path: path},                                        // Revision is invalid
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%+v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.TreeEntry(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainTreeEntryResponse(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func getTreeEntryFromTreeEntryClient(t *testing.T, client gitalypb.CommitService_TreeEntryClient) *treeEntry {
	fetchedTreeEntry := &treeEntry{}
	firstResponseReceived := false

	for {
		resp, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if !firstResponseReceived {
			firstResponseReceived = true
			fetchedTreeEntry.oid = resp.GetOid()
			fetchedTreeEntry.size = resp.GetSize()
			fetchedTreeEntry.mode = resp.GetMode()
			fetchedTreeEntry.objectType = resp.GetType()
		}
		fetchedTreeEntry.data = append(fetchedTreeEntry.data, resp.GetData()...)
	}

	return fetchedTreeEntry
}

func assertExactReceivedTreeEntry(t *testing.T, client gitalypb.CommitService_TreeEntryClient, expectedTreeEntry *treeEntry) {
	fetchedTreeEntry := getTreeEntryFromTreeEntryClient(t, client)

	if fetchedTreeEntry.oid != expectedTreeEntry.oid {
		t.Errorf("Expected tree entry OID to be %q, got %q", expectedTreeEntry.oid, fetchedTreeEntry.oid)
	}

	if fetchedTreeEntry.objectType != expectedTreeEntry.objectType {
		t.Errorf("Expected tree entry object type to be %d, got %d", expectedTreeEntry.objectType, fetchedTreeEntry.objectType)
	}

	if !bytes.Equal(fetchedTreeEntry.data, expectedTreeEntry.data) {
		t.Errorf("Expected tree entry data to be %q, got %q", expectedTreeEntry.data, fetchedTreeEntry.data)
	}

	if fetchedTreeEntry.size != expectedTreeEntry.size {
		t.Errorf("Expected tree entry size to be %d, got %d", expectedTreeEntry.size, fetchedTreeEntry.size)
	}

	if fetchedTreeEntry.mode != expectedTreeEntry.mode {
		t.Errorf("Expected tree entry mode to be %o, got %o", expectedTreeEntry.mode, fetchedTreeEntry.mode)
	}
}

func drainTreeEntryResponse(c gitalypb.CommitService_TreeEntryClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
