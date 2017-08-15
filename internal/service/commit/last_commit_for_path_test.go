package commit

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulLastCommitForPathRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	commit := &pb.GitCommit{
		Id:      "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
		Subject: []byte("Change some files"),
		Body:    []byte("Change some files\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
		Author: &pb.CommitAuthor{
			Name:  []byte("Dmitriy Zaporozhets"),
			Email: []byte("dmitriy.zaporozhets@gmail.com"),
			Date:  &timestamp.Timestamp{Seconds: 1393491451},
		},
		Committer: &pb.CommitAuthor{
			Name:  []byte("Dmitriy Zaporozhets"),
			Email: []byte("dmitriy.zaporozhets@gmail.com"),
			Date:  &timestamp.Timestamp{Seconds: 1393491451},
		},
		ParentIds: []string{"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"},
	}

	testCases := []struct {
		desc     string
		revision string
		path     []byte
		commit   *pb.GitCommit
	}{
		{
			desc:     "path present",
			revision: "e63f41fe459e62e1228fcef60d7189127aeba95a",
			path:     []byte("files/ruby/regex.rb"),
			commit:   commit,
		},
		{
			desc:     "path empty",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			commit:   commit,
		},
		{
			desc:     "file does not exist in this commit",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("files/lfs/lfs_object.iso"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &pb.LastCommitForPathRequest{
				Repository: testRepo,
				Revision:   []byte(testCase.revision),
				Path:       []byte(testCase.path),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.LastCommitForPath(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, testCase.commit, response.GetCommit(), "mismatched commits")
		})
	}
}

func TestFailedLastCommitForPathRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *pb.LastCommitForPathRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &pb.LastCommitForPathRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &pb.LastCommitForPathRequest{Revision: []byte("some-branch")},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Revision is missing",
			request: &pb.LastCommitForPathRequest{Repository: testRepo, Path: []byte("foo/bar")},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.LastCommitForPath(ctx, testCase.request)
			testhelper.AssertGrpcError(t, err, testCase.code, "")
		})
	}
}
