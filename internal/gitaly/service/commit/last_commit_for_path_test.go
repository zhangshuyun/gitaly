package commit

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulLastCommitForPathRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commit := &gitalypb.GitCommit{
		Id:      "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
		Subject: []byte("Change some files"),
		Body:    []byte("Change some files\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Dmitriy Zaporozhets"),
			Email:    []byte("dmitriy.zaporozhets@gmail.com"),
			Date:     &timestamp.Timestamp{Seconds: 1393491451},
			Timezone: []byte("+0200"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     []byte("Dmitriy Zaporozhets"),
			Email:    []byte("dmitriy.zaporozhets@gmail.com"),
			Date:     &timestamp.Timestamp{Seconds: 1393491451},
			Timezone: []byte("+0200"),
		},
		ParentIds:     []string{"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"},
		BodySize:      86,
		SignatureType: gitalypb.SignatureType_PGP,
	}

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
				Repository: testRepo,
				Revision:   []byte(testCase.revision),
				Path:       testCase.path,
			}

			ctx, cancel := testhelper.Context()
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
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
			request: &gitalypb.LastCommitForPathRequest{Repository: testRepo, Path: []byte("foo/bar")},
			code:    codes.InvalidArgument,
		},
		{
			desc: "Revision is invalid",
			request: &gitalypb.LastCommitForPathRequest{
				Repository: testRepo, Path: []byte("foo/bar"), Revision: []byte("--output=/meow"),
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
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	// This is an arbitrary blob known to exist in the test repository
	const blobID = "c60514b6d3d6bf4bec1030f70026e34dfbd69ad5"
	path := ":wq"

	commitID := testhelper.CommitBlobWithName(t,
		testRepoPath,
		blobID,
		path,
		"commit for filename with glob characters",
	)

	request := &gitalypb.LastCommitForPathRequest{
		Repository:      testRepo,
		Revision:        []byte(commitID),
		Path:            []byte(path),
		LiteralPathspec: true,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	response, err := client.LastCommitForPath(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response.GetCommit())
	require.Equal(t, commitID, response.GetCommit().Id)

	request.LiteralPathspec = false
	response, err = client.LastCommitForPath(ctx, request)
	require.NoError(t, err)
	require.Nil(t, response.GetCommit())
}
