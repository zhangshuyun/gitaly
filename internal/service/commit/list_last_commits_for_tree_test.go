package commit

import (
	"bytes"
	"context"
	"io"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type commitInfo struct {
	path []byte
	id   string
}

func TestSuccessfulListLastCommitsForTreeRequest(t *testing.T) {
	server, serverSockerPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSockerPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		revision string
		path     []byte
		info     []commitInfo
		limit    int32
		offset   int32
	}{
		{
			desc:     "path is '/'",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("/"),
			info: []commitInfo{
				{
					path: []byte("encoding"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("files"),
					id:   "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
				},
				{
					path: []byte(".gitignore"),
					id:   "c1acaa58bbcbc3eafe538cb8274ba387047b69f8",
				},
				{
					path: []byte(".gitmodules"),
					id:   "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				},
				{
					path: []byte("CHANGELOG"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("CONTRIBUTING.md"),
					id:   "6d394385cf567f80a8fd85055db1ab4c5295806f",
				},
				{
					path: []byte("Gemfile.zip"),
					id:   "ae73cb07c9eeaf35924a10f713b364d32b2dd34f",
				},
				{
					path: []byte("LICENSE"),
					id:   "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
				},
				{
					path: []byte("MAINTENANCE.md"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("PROCESS.md"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("README.md"),
					id:   "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
				},
				{
					path: []byte("VERSION"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("gitlab-shell"),
					id:   "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				},
				{
					path: []byte("six"),
					id:   "cfe32cf61b73a0d5e9f13e774abde7ff789b1660",
				},
			},
			limit:  25,
			offset: 0,
		},
		{
			desc:     "path is 'files/'",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("files/"),
			info: []commitInfo{
				{
					path: []byte("files/html"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("files/images"),
					id:   "2f63565e7aac07bcdadb654e253078b727143ec4",
				},
				{
					path: []byte("files/js"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("files/markdown"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
				{
					path: []byte("files/ruby"),
					id:   "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
				},
			},
			limit:  25,
			offset: 0,
		},
		{
			desc:     "with offset higher than number of paths",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("/"),
			info:     []commitInfo{},
			limit:    25,
			offset:   14,
		},
		{
			desc:     "with limit 1",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("/"),
			info: []commitInfo{
				{
					path: []byte("encoding"),
					id:   "913c66a37b4a45b9769037c55c2d238bd0942d2e",
				},
			},
			limit:  1,
			offset: 0,
		},
		{
			desc:     "with offset 13",
			revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			path:     []byte("/"),
			info: []commitInfo{
				{
					path: []byte("six"),
					id:   "cfe32cf61b73a0d5e9f13e774abde7ff789b1660",
				},
			},
			limit:  25,
			offset: 13,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.ListLastCommitsForTreeRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
				Path:       testCase.path,
				Limit:      testCase.limit,
				Offset:     testCase.offset,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.ListLastCommitsForTree(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			counter := 0
			for {
				fetchedCommits, err := stream.Recv()
				if err == io.EOF {
					break
				}

				require.NoError(t, err)

				commits := fetchedCommits.GetCommits()

				for _, fetchedCommit := range commits {
					expectedInfo := testCase.info[counter]

					require.Equal(t, expectedInfo.path, fetchedCommit.PathBytes)
					require.Equal(t, expectedInfo.id, fetchedCommit.Commit.Id)

					counter++
				}
			}
		})
	}
}

func TestFailedListLastCommitsForTreeRequest(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	invalidRepo := &gitalypb.Repository{StorageName: "broken", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.ListLastCommitsForTreeRequest
		code    codes.Code
	}{
		{
			desc: "Revision is missing",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Repository: testRepo,
				Path:       []byte("/"),
				Revision:   "",
				Offset:     0,
				Limit:      25,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Invalid repository",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Repository: invalidRepo,
				Path:       []byte("/"),
				Revision:   "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
				Offset:     0,
				Limit:      25,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Path:     []byte("/"),
				Revision: "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
				Offset:   0,
				Limit:    25,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Revision is missing",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Repository: testRepo,
				Path:       []byte("/"),
				Offset:     0,
				Limit:      25,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Ambiguous revision",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Repository: testRepo,
				Revision:   "a",
				Offset:     0,
				Limit:      25,
			},
			code: codes.Internal,
		},
		{
			desc: "Invalid revision",
			request: &gitalypb.ListLastCommitsForTreeRequest{
				Repository: testRepo,
				Revision:   "--output=/meow",
				Offset:     0,
				Limit:      25,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		ctx, cancel := testhelper.Context()
		defer cancel()

		stream, err := client.ListLastCommitsForTree(ctx, testCase.request)
		require.NoError(t, err)

		t.Run(testCase.desc, func(t *testing.T) {
			_, err := stream.Recv()

			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestNonUtf8ListLastCommitsForTreeRequest(t *testing.T) {
	server, serverSockerPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSockerPath)
	defer conn.Close()
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// This is an arbitrary blob known to exist in the test repository
	const blobID = "c60514b6d3d6bf4bec1030f70026e34dfbd69ad5"

	nonUTF8Filename := "hello\x80world"
	require.False(t, utf8.ValidString(nonUTF8Filename))

	commitID := testhelper.CommitBlobWithName(t,
		testRepoPath,
		blobID,
		nonUTF8Filename,
		"commit for non-utf8 path",
	)

	request := &gitalypb.ListLastCommitsForTreeRequest{
		Repository: testRepo,
		Revision:   string(commitID),
		Limit:      100,
		Offset:     0,
	}

	stream, err := client.ListLastCommitsForTree(ctx, request)
	require.NoError(t, err)

	var nonUTF8FilenameFound bool
	for {
		fetchedCommits, err := stream.Recv()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		commits := fetchedCommits.GetCommits()

		for _, fetchedCommit := range commits {
			if bytes.Equal(fetchedCommit.PathBytes, []byte(nonUTF8Filename)) {
				nonUTF8FilenameFound = true
			}
		}
	}

	assert.True(t, nonUTF8FilenameFound)
}
