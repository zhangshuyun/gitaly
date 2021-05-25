package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulLastCommitForPathRequest(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	commit := testhelper.GitLabTestCommit("570e7b2abdd848b95f2f578043fc23bd6f6fd24d")

	testCases := []struct {
		desc     string
		revision string
		path     []byte
		commit   *gitalypb.GitCommit
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
			desc:     "path is '/'",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			commit:   commit,
			path:     []byte("/"),
		},
		{
			desc:     "path is '*'",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			commit:   commit,
			path:     []byte("*"),
		},
		{
			desc:     "file does not exist in this commit",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("files/lfs/lfs_object.iso"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.LastCommitForPathRequest{
				Repository: repo,
				Revision:   []byte(testCase.revision),
				Path:       testCase.path,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.LastCommitForPath(ctx, request)
			require.NoError(t, err)

			testhelper.ProtoEqual(t, testCase.commit, response.GetCommit())
		})
	}
}

func TestFailedLastCommitForPathRequest(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.LastCommitForPathRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.LastCommitForPathRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.LastCommitForPathRequest{Revision: []byte("some-branch")},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Revision is missing",
			request: &gitalypb.LastCommitForPathRequest{Repository: repo, Path: []byte("foo/bar")},
			code:    codes.InvalidArgument,
		},
		{
			desc: "Revision is invalid",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: repo, Path: []byte("foo/bar"), Revision: []byte("--output=/meow"),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.LastCommitForPath(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestSuccessfulLastCommitWithGlobCharacters(t *testing.T) {
	cfg, repo, repoPath, client := setupCommitServiceWithRepo(t, true)

	// This is an arbitrary blob known to exist in the test repository
	const blobID = "c60514b6d3d6bf4bec1030f70026e34dfbd69ad5"
	path := ":wq"

	commitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithTreeEntries(gittest.TreeEntry{
			Mode: "100644", Path: path, OID: git.ObjectID(blobID),
		}),
	)

	request := &gitalypb.LastCommitForPathRequest{
		Repository:      repo,
		Revision:        []byte(commitID),
		Path:            []byte(path),
		LiteralPathspec: true,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	response, err := client.LastCommitForPath(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response.GetCommit())
	require.Equal(t, commitID.String(), response.GetCommit().Id)

	request.LiteralPathspec = false
	response, err = client.LastCommitForPath(ctx, request)
	require.NoError(t, err)
	require.Nil(t, response.GetCommit())
}
