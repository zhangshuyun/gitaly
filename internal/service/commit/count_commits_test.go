package commit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCountCommitsRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo1, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testRepo2, testRepo2Path, cleanupFn := testhelper.InitRepoWithWorktree(t)
	defer cleanupFn()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	for i := 0; i < 5; i++ {
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepo2Path,
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"commit", "--allow-empty", "-m", "Empty commit")
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepo2Path, "checkout", "-b", "another-branch")

	for i := 0; i < 3; i++ {
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepo2Path,
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"commit", "--allow-empty", "-m", "Empty commit")
	}

	testCases := []struct {
		repo                *gitalypb.Repository
		revision, path      []byte
		all                 bool
		before, after, desc string
		maxCount            int32
		count               int32
	}{
		{
			desc:     "revision only #1",
			repo:     testRepo1,
			revision: []byte("1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"),
			count:    1,
		},
		{
			desc:     "revision only #2",
			repo:     testRepo1,
			revision: []byte("6d394385cf567f80a8fd85055db1ab4c5295806f"),
			count:    2,
		},
		{
			desc:     "revision only #3",
			repo:     testRepo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			count:    39,
		},
		{
			desc:     "revision + max-count",
			repo:     testRepo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			maxCount: 15,
			count:    15,
		},
		{
			desc:     "non-existing revision",
			repo:     testRepo1,
			revision: []byte("deadfacedeadfacedeadfacedeadfacedeadface"),
			count:    0,
		},
		{
			desc:     "revision + before",
			repo:     testRepo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			count:    26,
		},
		{
			desc:     "revision + before + after",
			repo:     testRepo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			count:    23,
		},
		{
			desc:     "revision + before + after + path",
			repo:     testRepo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			path:     []byte("files"),
			count:    12,
		},
		{
			desc:  "all refs #1",
			repo:  testRepo2,
			all:   true,
			count: 8,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {

			request := &gitalypb.CountCommitsRequest{Repository: testCase.repo}

			if testCase.all {
				request.All = true
			} else {
				request.Revision = testCase.revision
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

			if testCase.maxCount != 0 {
				request.MaxCount = testCase.maxCount
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
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	revision := []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72")

	rpcRequests := []gitalypb.CountCommitsRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: revision}, // Repository doesn't exist
		{Repository: nil, Revision: revision},                                  // Repository is nil
		{Repository: testRepo, Revision: nil, All: false},                      // Revision is empty and All is false
		{Repository: testRepo, Revision: []byte("--output=/meow"), All: false}, // Revision is invalid
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.CountCommits(ctx, &rpcRequest)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
