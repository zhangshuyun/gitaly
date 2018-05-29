package conflicts

import (
	"io"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type conflictFile struct {
	header  *pb.ConflictFileHeader
	content []byte
}

func TestSuccessfulListConflictFilesRequest(t *testing.T) {
	server, serverSocketPath := runConflictsServer(t)
	defer server.Stop()

	client, conn := NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ourCommitOid := "1a35b5a77cf6af7edf6703f88e82f6aff613666f"
	theirCommitOid := "8309e68585b28d61eb85b7e2834849dda6bf1733"

	conflictContent1 := `<<<<<<< encoding/codagé
Content is not important, file name is
=======
Content can be important, but here, file name is of utmost importance
>>>>>>> encoding/codagé
`
	conflictContent2 := `<<<<<<< files/ruby/feature.rb
class Feature
  def foo
    puts 'bar'
  end
=======
# This file was changed in feature branch
# We put different code here to make merge conflict
class Conflict
>>>>>>> files/ruby/feature.rb
end
`

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &pb.ListConflictFilesRequest{
		Repository:     testRepo,
		OurCommitOid:   ourCommitOid,
		TheirCommitOid: theirCommitOid,
	}

	c, err := client.ListConflictFiles(ctx, request)
	if err != nil {
		t.Fatal(err)
	}

	expectedFiles := []*conflictFile{
		{
			header: &pb.ConflictFileHeader{
				Repository: testRepo,
				CommitOid:  ourCommitOid,
				OurMode:    int32(0100644),
				OurPath:    []byte("encoding/codagé"),
				TheirPath:  []byte("encoding/codagé"),
			},
			content: []byte(conflictContent1),
		},
		{
			header: &pb.ConflictFileHeader{
				Repository: testRepo,
				CommitOid:  ourCommitOid,
				OurMode:    int32(0100644),
				OurPath:    []byte("files/ruby/feature.rb"),
				TheirPath:  []byte("files/ruby/feature.rb"),
			},
			content: []byte(conflictContent2),
		},
	}

	receivedFiles := getConflictFiles(t, c)
	require.Equal(t, expectedFiles, receivedFiles)
}

func TestFailedListConflictFilesRequestDueToConflictSideMissing(t *testing.T) {
	server, serverSocketPath := runConflictsServer(t)
	defer server.Stop()

	client, conn := NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ourCommitOid := "eb227b3e214624708c474bdab7bde7afc17cefcc" // conflict-missing-side
	theirCommitOid := "824be604a34828eb682305f0d963056cfac87b2d"

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &pb.ListConflictFilesRequest{
		Repository:     testRepo,
		OurCommitOid:   ourCommitOid,
		TheirCommitOid: theirCommitOid,
	}

	c, _ := client.ListConflictFiles(ctx, request)
	testhelper.RequireGrpcError(t, drainListConflictFilesResponse(c), codes.FailedPrecondition)
}

func TestFailedListConflictFilesRequestDueToValidation(t *testing.T) {
	server, serverSocketPath := runConflictsServer(t)
	defer server.Stop()

	client, conn := NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ourCommitOid := "0b4bc9a49b562e85de7cc9e834518ea6828729b9"
	theirCommitOid := "bb5206fee213d983da88c47f9cf4cc6caf9c66dc"

	testCases := []struct {
		desc    string
		request *pb.ListConflictFilesRequest
		code    codes.Code
	}{
		{
			desc: "empty repo",
			request: &pb.ListConflictFilesRequest{
				Repository:     nil,
				OurCommitOid:   ourCommitOid,
				TheirCommitOid: theirCommitOid,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty OurCommitId repo",
			request: &pb.ListConflictFilesRequest{
				Repository:     testRepo,
				OurCommitOid:   "",
				TheirCommitOid: theirCommitOid,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty TheirCommitId repo",
			request: &pb.ListConflictFilesRequest{
				Repository:     testRepo,
				OurCommitOid:   ourCommitOid,
				TheirCommitOid: "",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, _ := client.ListConflictFiles(ctx, testCase.request)
			testhelper.RequireGrpcError(t, drainListConflictFilesResponse(c), testCase.code)
		})
	}
}

func getConflictFiles(t *testing.T, c pb.ConflictsService_ListConflictFilesClient) []*conflictFile {
	files := []*conflictFile{}
	var currentFile *conflictFile

	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		for _, file := range r.GetFiles() {
			// If there's a header this is the beginning of a new file
			if header := file.GetHeader(); header != nil {
				if currentFile != nil {
					files = append(files, currentFile)
				}

				currentFile = &conflictFile{header: header}
			} else {
				// Append to current file's content
				currentFile.content = append(currentFile.content, file.GetContent()...)
			}
		}
	}

	// Append leftover file
	files = append(files, currentFile)

	return files
}

func drainListConflictFilesResponse(c pb.ConflictsService_ListConflictFilesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
