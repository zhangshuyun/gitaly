package commit

import (
	"fmt"
	"io"
	"testing"

	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestSuccessfulGetTreeEntries(t *testing.T) {
	//  Force entries to be sliced to test that behaviour
	oldMaxTreeEntries := maxTreeEntries
	maxTreeEntries = 1
	defer func() {
		maxTreeEntries = oldMaxTreeEntries
	}()

	commitID := "913c66a37b4a45b9769037c55c2d238bd0942d2e"
	rootOid := "faafbe7fe23fb83c664c78aaded9566c8f934412"

	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	rootEntries := []*pb.TreeEntry{
		{
			Oid:       "fd90a3d2d21d6b4f9bec2c33fb7f49780c55f0d2",
			RootOid:   rootOid,
			Path:      []byte(".DS_Store"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "470ad2fcf1e33798f1afc5781d08e60c40f51e7a",
			RootOid:   rootOid,
			Path:      []byte(".gitignore"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "fdaada1754989978413d618ee1fb1c0469d6a664",
			RootOid:   rootOid,
			Path:      []byte(".gitmodules"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "c74175afd117781cbc983664339a0f599b5bb34e",
			RootOid:   rootOid,
			Path:      []byte("CHANGELOG"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "c1788657b95998a2f177a4f86d68a60f2a80117f",
			RootOid:   rootOid,
			Path:      []byte("CONTRIBUTING.md"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "50b27c6518be44c42c4d87966ae2481ce895624c",
			RootOid:   rootOid,
			Path:      []byte("LICENSE"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			RootOid:   rootOid,
			Path:      []byte("MAINTENANCE.md"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "bf757025c40c62e6ffa6f11d3819c769a76dbe09",
			RootOid:   rootOid,
			Path:      []byte("PROCESS.md"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "faaf198af3a36dbf41961466703cc1d47c61d051",
			RootOid:   rootOid,
			Path:      []byte("README.md"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "998707b421c89bd9a3063333f9f728ef3e43d101",
			RootOid:   rootOid,
			Path:      []byte("VERSION"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "3c122d2b7830eca25235131070602575cf8b41a1",
			RootOid:   rootOid,
			Path:      []byte("encoding"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "ab746f8ad0b84b147290041dc13cc9c7adc52930",
			RootOid:   rootOid,
			Path:      []byte("files"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "409f37c4f05865e4fb208c771485f211a22c4c2d",
			RootOid:   rootOid,
			Path:      []byte("six"),
			Type:      pb.TreeEntry_COMMIT,
			Mode:      0160000,
			CommitOid: commitID,
		},
	}
	filesDirEntries := []*pb.TreeEntry{
		{
			Oid:       "60d7a906c2fd9e4509aeb1187b98d0ea7ce827c9",
			RootOid:   rootOid,
			Path:      []byte("files/.DS_Store"),
			Type:      pb.TreeEntry_BLOB,
			Mode:      0100644,
			CommitOid: commitID,
		},
		{
			Oid:       "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
			RootOid:   rootOid,
			Path:      []byte("files/html"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "5e147e3af6740ee83103ec2ecdf846cae696edd1",
			RootOid:   rootOid,
			Path:      []byte("files/images"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "7853101769f3421725ddc41439c2cd4610e37ad9",
			RootOid:   rootOid,
			Path:      []byte("files/js"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "fd581c619bf59cfdfa9c8282377bb09c2f897520",
			RootOid:   rootOid,
			Path:      []byte("files/markdown"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
		{
			Oid:       "b59dbe4a27371d53e61bf3cb8bef66be53572db0",
			RootOid:   rootOid,
			Path:      []byte("files/ruby"),
			Type:      pb.TreeEntry_TREE,
			Mode:      040000,
			CommitOid: commitID,
		},
	}

	testCases := []struct {
		description string
		revision    []byte
		path        []byte
		entries     []*pb.TreeEntry
	}{
		{
			description: "with root path",
			revision:    []byte(commitID),
			path:        []byte("."),
			entries:     rootEntries,
		},
		{
			description: "with a folder",
			revision:    []byte(commitID),
			path:        []byte("files"),
			entries:     filesDirEntries,
		},
		{
			description: "with a file",
			revision:    []byte(commitID),
			path:        []byte(".gitignore"),
			entries:     []*pb.TreeEntry{},
		},
		{
			description: "with a non-existing path",
			revision:    []byte(commitID),
			path:        []byte("i-dont/exist"),
			entries:     []*pb.TreeEntry{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &pb.GetTreeEntriesRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
				Path:       testCase.path,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.GetTreeEntries(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			assertTreeEntriesReceived(t, c, testCase.entries)
		})
	}
}

func assertTreeEntriesReceived(t *testing.T, client pb.CommitService_GetTreeEntriesClient, entries []*pb.TreeEntry) {
	fetchedEntries := getTreeEntriesFromTreeEntryClient(t, client)

	if len(fetchedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d instead", len(entries), len(fetchedEntries))
	}

	for i, entry := range fetchedEntries {
		if !treeEntriesEqual(entry, entries[i]) {
			t.Fatalf("Expected tree entry %v, got %v instead", entries[i], entry)
		}
	}
}

func getTreeEntriesFromTreeEntryClient(t *testing.T, client pb.CommitService_GetTreeEntriesClient) []*pb.TreeEntry {
	var entries []*pb.TreeEntry
	for {
		resp, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		entries = append(entries, resp.Entries...)
	}
	return entries
}

func TestFailedGetTreeEntriesRequestDueToValidationError(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	revision := []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72")
	path := []byte("a/b/c")

	rpcRequests := []pb.GetTreeEntriesRequest{
		{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: revision, Path: path}, // Repository doesn't exist
		{Repository: nil, Revision: revision, Path: path},                                                       // Repository is nil
		{Repository: testRepo, Revision: nil, Path: path},                                                       // Revision is empty
		{Repository: testRepo, Revision: revision},                                                              // Path is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.GetTreeEntries(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainTreeEntriesResponse(c)
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
		})
	}
}

func drainTreeEntriesResponse(c pb.CommitService_GetTreeEntriesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
