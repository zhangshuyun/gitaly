package commit

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCountCommitsRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		revision, path      []byte
		before, after, desc string
		count               int32
	}{
		{
			desc:     "revision only #1",
			revision: []byte("1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"),
			count:    1,
		},
		{
			desc:     "revision only #2",
			revision: []byte("6d394385cf567f80a8fd85055db1ab4c5295806f"),
			count:    2,
		},
		{
			desc:     "revision only #3",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			count:    39,
		},
		{
			desc:     "non-existing revision",
			revision: []byte("deadfacedeadfacedeadfacedeadfacedeadface"),
			count:    0,
		},
		{
			desc:     "revision + before",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			count:    26,
		},
		{
			desc:     "revision + before + after",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			count:    23,
		},
		{
			desc:     "revision + before + after + path",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			path:     []byte("files"),
			count:    12,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {

			request := &pb.CountCommitsRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
			}

			if testCase.before != "" {
				before, err := time.Parse(time.RFC3339, testCase.before)
				if err != nil {
					t.Fatal(err)
				}
				request.Before = &timestamp.Timestamp{Seconds: before.Unix()}
			}

			if testCase.after != "" {
				after, err := time.Parse(time.RFC3339, testCase.after)
				if err != nil {
					t.Fatal(err)
				}
				request.After = &timestamp.Timestamp{Seconds: after.Unix()}
			}

			if testCase.path != nil {
				request.Path = testCase.path
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.CountCommits(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, response.Count, testCase.count)
		})
	}
}

func TestFailedCountCommitsRequestDueToValidationError(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	revision := []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72")

	rpcRequests := []pb.CountCommitsRequest{
		{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: revision}, // Repository doesn't exist
		{Repository: nil, Revision: revision},                                                       // Repository is nil
		{Repository: testRepo, Revision: nil},                                                       // Revision is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.CountCommits(ctx, &rpcRequest)
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
		})
	}
}
