package blob

import (
	"bytes"
	"io"
	"net"
	"path"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var serverSocketPath = path.Join(scratchDir, "gitaly.sock")

type blob struct {
	id      string
	content []byte
	size    int32
	found   bool
}

func TestSuccessfulBlobFromCommit(t *testing.T) {
	server := runBlobServer(t)
	defer server.Stop()

	client := newBlobClient(t)
	repo := &pb.Repository{Path: testRepoPath}

	testCases := []struct {
		commit       string
		path         []byte
		limit        int
		expectedBlob blob
	}{
		{
			commit: "913c66a37b4a45b9769037c55c2d238bd0942d2e",
			path:   []byte("MAINTENANCE.md"),
			expectedBlob: blob{
				id:      "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
				content: testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt"),
				size:    1367,
				found:   true,
			},
		},
		{
			commit: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
			path:   []byte("with space/README.md"),
			expectedBlob: blob{
				id:      "8c3014aceae45386c3c026a7ea4a1f68660d51d6",
				content: testhelper.MustReadFile(t, "testdata/with-space-readme-md-blob.txt"),
				size:    36,
				found:   true,
			},
		},
		{
			commit: "deadfacedeadfacedeadfacedeadfacedeadface",
			path:   []byte("with space/README.md"),
			expectedBlob: blob{
				found: false,
			},
		},
		{
			commit: "e63f41fe459e62e1228fcef60d7189127aeba95a",
			path:   []byte("gitlab-grack"),
			expectedBlob: blob{
				id:    "645f6c4c82fd3f5e06f67134450a570b795e55a6",
				found: true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Logf("test case: commit=%q path=%q", testCase.commit, testCase.path)

		request := &pb.BlobFromCommitRequest{
			Repository: repo,
			CommitId:   testCase.commit,
			Path:       testCase.path,
		}

		c, err := client.BlobFromCommit(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}

		assertExactReceivedBlob(t, c, &testCase.expectedBlob)
	}
}

func runBlobServer(t *testing.T) *grpc.Server {
	server := grpc.NewServer()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterBlobServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newBlobClient(t *testing.T) pb.BlobClient {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewBlobClient(conn)
}

func getBlobFromBlobFromCommitClient(t *testing.T, client pb.Blob_BlobFromCommitClient) *blob {
	fetchedBlob := &blob{}

	for {
		resp, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		fetchedBlob.id = resp.GetId()
		fetchedBlob.found = resp.GetFound()
		fetchedBlob.size = resp.GetSize()
		fetchedBlob.content = append(fetchedBlob.content, resp.GetBlob()...)
	}

	return fetchedBlob
}

func assertExactReceivedBlob(t *testing.T, client pb.Blob_BlobFromCommitClient, expectedBlob *blob) {
	fetchedBlob := getBlobFromBlobFromCommitClient(t, client)

	if fetchedBlob.found != expectedBlob.found {
		t.Errorf("Expected blob existence to be %t, got %t", expectedBlob.found, fetchedBlob.found)
	}

	if fetchedBlob.id != expectedBlob.id {
		t.Errorf("Expected blob ID to be %q, got %q", expectedBlob.id, fetchedBlob.id)
	}

	if !bytes.Equal(fetchedBlob.content, expectedBlob.content) {
		t.Errorf("Expected blob content to be %q, got %q", expectedBlob.content, fetchedBlob.content)
	}

	if fetchedBlob.size != expectedBlob.size {
		t.Errorf("Expected blob size to be %d, got %d", expectedBlob.size, fetchedBlob.size)
	}
}
