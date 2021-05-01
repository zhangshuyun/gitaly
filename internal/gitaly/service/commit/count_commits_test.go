package commit

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCountCommitsRequest(t *testing.T) {
	cfg, repo1, _, client := setupCommitServiceWithRepo(t, true)

	repo2, repo2Path, cleanupFn := gittest.InitRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	t.Cleanup(cleanupFn)

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	for i := 0; i < 5; i++ {
		gittest.Exec(t, cfg, "-C", repo2Path,
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"commit", "--allow-empty", "-m", "Empty commit")
	}

	gittest.Exec(t, cfg, "-C", repo2Path, "checkout", "-b", "another-branch")

	for i := 0; i < 3; i++ {
		gittest.Exec(t, cfg, "-C", repo2Path,
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"commit", "--allow-empty", "-m", "Empty commit")
	}

	literalOptions := &gitalypb.GlobalOptions{LiteralPathspecs: true}

	testCases := []struct {
		repo                *gitalypb.Repository
		revision, path      []byte
		all                 bool
		options             *gitalypb.GlobalOptions
		before, after, desc string
		maxCount            int32
		count               int32
	}{
		{
			desc:     "revision only #1",
			repo:     repo1,
			revision: []byte("1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"),
			count:    1,
		},
		{
			desc:     "revision only #2",
			repo:     repo1,
			revision: []byte("6d394385cf567f80a8fd85055db1ab4c5295806f"),
			count:    2,
		},
		{
			desc:     "revision only #3",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			count:    39,
		},
		{
			desc:     "revision + max-count",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			maxCount: 15,
			count:    15,
		},
		{
			desc:     "non-existing revision",
			repo:     repo1,
			revision: []byte("deadfacedeadfacedeadfacedeadfacedeadface"),
			count:    0,
		},
		{
			desc:     "revision + before",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			count:    26,
		},
		{
			desc:     "revision + before + after",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			count:    23,
		},
		{
			desc:     "revision + before + after + path",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			path:     []byte("files"),
			count:    12,
		},
		{
			desc:     "revision + before + after + wildcard path",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			path:     []byte("files/*"),
			count:    12,
		},
		{
			desc:     "revision + before + after + non-existent literal pathspec",
			repo:     repo1,
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			before:   "2015-12-07T11:54:28+01:00",
			after:    "2014-02-27T10:14:56+02:00",
			path:     []byte("files/*"),
			options:  literalOptions,
			count:    0,
		},
		{
			desc:  "all refs #1",
			repo:  repo2,
			all:   true,
			count: 8,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.CountCommitsRequest{Repository: testCase.repo, GlobalOptions: testCase.options}

			if testCase.all {
				request.All = true
			} else {
				request.Revision = testCase.revision
			}

			if testCase.before != "" {
				before, err := time.Parse(time.RFC3339, testCase.before)
				require.NoError(t, err)
				request.Before = &timestamp.Timestamp{Seconds: before.Unix()}
			}

			if testCase.after != "" {
				after, err := time.Parse(time.RFC3339, testCase.after)
				require.NoError(t, err)
				request.After = &timestamp.Timestamp{Seconds: after.Unix()}
			}

			if testCase.maxCount != 0 {
				request.MaxCount = testCase.maxCount
			}

			if testCase.path != nil {
				request.Path = testCase.path
			}

			ctx, cancel := testhelper.Context()
			defer cancel()
			response, err := client.CountCommits(ctx, request)
			require.NoError(t, err)
			require.Equal(t, response.Count, testCase.count)
		})
	}
}

func TestFailedCountCommitsRequestDueToValidationError(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	revision := []byte("d42783470dc29fde2cf459eb3199ee1d7e3f3a72")

	rpcRequests := []*gitalypb.CountCommitsRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, Revision: revision}, // Repository doesn't exist
		{Repository: nil, Revision: revision},                              // Repository is nil
		{Repository: repo, Revision: nil, All: false},                      // Revision is empty and All is false
		{Repository: repo, Revision: []byte("--output=/meow"), All: false}, // Revision is invalid
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.CountCommits(ctx, rpcRequest)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
