package diff

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindChangedPathsRequest_success(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc          string
		commits       []string
		expectedPaths []*gitalypb.ChangedPaths
	}{
		{
			"Returns the expected results without a merge commit",
			[]string{"e4003da16c1c2c3fc4567700121b17bf8e591c6c", "57290e673a4c87f51294f5216672cbc58d485d25", "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab", "d59c60028b053793cecfb4022de34602e1a9218e"},
			[]*gitalypb.ChangedPaths{
				{
					Status: gitalypb.ChangedPaths_MODIFIED,
					Path:   []byte("CONTRIBUTING.md"),
				},
				{
					Status: gitalypb.ChangedPaths_MODIFIED,
					Path:   []byte("MAINTENANCE.md"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/テスト.txt"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/deleted-file"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/file-with-multiple-chunks"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/mode-file"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/mode-file-with-mods"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/named-file"),
				},
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("gitaly/named-file-with-mods"),
				},
				{
					Status: gitalypb.ChangedPaths_DELETED,
					Path:   []byte("files/js/commit.js.coffee"),
				},
			},
		},
		{
			"Returns the expected results with a merge commit",
			[]string{"7975be0116940bf2ad4321f79d02a55c5f7779aa", "55bc176024cfa3baaceb71db584c7e5df900ea65"},
			[]*gitalypb.ChangedPaths{
				{
					Status: gitalypb.ChangedPaths_ADDED,
					Path:   []byte("files/images/emoji.png"),
				},
				{
					Status: gitalypb.ChangedPaths_MODIFIED,
					Path:   []byte(".gitattributes"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := &gitalypb.FindChangedPathsRequest{Repository: testRepo, Commits: tc.commits}

			stream, err := client.FindChangedPaths(ctx, rpcRequest)
			require.NoError(t, err)

			var paths []*gitalypb.ChangedPaths
			for {
				fetchedPaths, err := stream.Recv()
				if err == io.EOF {
					break
				}

				require.NoError(t, err)

				paths = append(paths, fetchedPaths.GetPaths()...)
			}

			require.Equal(t, tc.expectedPaths, paths)
		})
	}
}

func TestFindChangedPathsRequest_failing(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	tests := []struct {
		desc    string
		repo    *gitalypb.Repository
		commits []string
		err     error
	}{
		{
			desc:    "Repo not found",
			repo:    &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar.git"},
			commits: []string{"e4003da16c1c2c3fc4567700121b17bf8e591c6c", "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"},
			err:     status.Errorf(codes.NotFound, "GetRepoPath: not a git repository: %q", filepath.Join(testhelper.GitlabTestStoragePath(), "bar.git")),
		},
		{
			desc:    "Storage not found",
			repo:    &gitalypb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			commits: []string{"e4003da16c1c2c3fc4567700121b17bf8e591c6c", "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"},
			err:     status.Error(codes.InvalidArgument, "GetStorageByName: no such storage: \"foo\""),
		},
		{
			desc:    "Commits cannot contain an empty commit",
			repo:    testRepo,
			commits: []string{""},
			err:     status.Error(codes.InvalidArgument, "FindChangedPaths: commits cannot contain an empty commit"),
		},
		{
			desc:    "Invalid commit",
			repo:    testRepo,
			commits: []string{"invalidinvalidinvalid", "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"},
			err:     status.Error(codes.NotFound, "FindChangedPaths: commit: invalidinvalidinvalid can not be found"),
		},
		{
			desc:    "Commit not found",
			repo:    testRepo,
			commits: []string{"z4003da16c1c2c3fc4567700121b17bf8e591c6c", "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"},
			err:     status.Error(codes.NotFound, "FindChangedPaths: commit: z4003da16c1c2c3fc4567700121b17bf8e591c6c can not be found"),
		},
	}

	for _, tc := range tests {
		rpcRequest := &gitalypb.FindChangedPathsRequest{Repository: tc.repo, Commits: tc.commits}
		stream, err := client.FindChangedPaths(ctx, rpcRequest)
		require.NoError(t, err)

		t.Run(tc.desc, func(t *testing.T) {
			_, err := stream.Recv()
			require.Equal(t, tc.err, err)
		})
	}
}
