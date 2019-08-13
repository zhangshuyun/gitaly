package commit

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulRawBlameRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		revision, path, data []byte
	}{
		{
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			path:     []byte("files/ruby/popen.rb"),
			data:     testhelper.MustReadFile(t, "testdata/files-ruby-popen-e63f41f-blame.txt"),
		},
		{
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			path:     []byte("files/ruby/../ruby/popen.rb"),
			data:     testhelper.MustReadFile(t, "testdata/files-ruby-popen-e63f41f-blame.txt"),
		},
		{
			revision: []byte("93dcf076a236c837dd47d61f86d95a6b3d71b586"),
			path:     []byte("gitaly/empty-file"),
			data:     []byte{},
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("test case: revision=%q path=%q", testCase.revision, testCase.path), func(t *testing.T) {

			request := &gitalypb.RawBlameRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
				Path:       testCase.path,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.RawBlame(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			sr := streamio.NewReader(func() ([]byte, error) {
				response, err := c.Recv()
				return response.GetData(), err
			})

			blame, err := ioutil.ReadAll(sr)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, testCase.data, blame, "blame data mismatched")
		})
	}
}

func TestFailedRawBlameRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description    string
		repo           *gitalypb.Repository
		revision, path []byte
		code           codes.Code
	}{
		{
			description: "Invalid repo",
			repo:        invalidRepo,
			revision:    []byte("master"),
			path:        []byte("a/b/c"),
			code:        codes.InvalidArgument,
		},
		{
			description: "Empty revision",
			repo:        testRepo,
			revision:    []byte(""),
			path:        []byte("a/b/c"),
			code:        codes.InvalidArgument,
		},
		{
			description: "Empty path",
			repo:        testRepo,
			revision:    []byte("abcdef"),
			path:        []byte(""),
			code:        codes.InvalidArgument,
		},
		{
			description: "Invalid revision",
			repo:        testRepo,
			revision:    []byte("--output=/meow"),
			path:        []byte("a/b/c"),
			code:        codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {

			request := gitalypb.RawBlameRequest{
				Repository: testCase.repo,
				Revision:   testCase.revision,
				Path:       testCase.path,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.RawBlame(ctx, &request)
			if err != nil {
				t.Fatal(err)
			}

			testhelper.RequireGrpcError(t, drainRawBlameResponse(c), testCase.code)
		})
	}
}

func drainRawBlameResponse(c gitalypb.CommitService_RawBlameClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
