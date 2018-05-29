package ref

import (
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestSuccessfulDeleteRefs(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *pb.DeleteRefsRequest
	}{
		{
			desc: "delete all except refs with certain prefixes",
			request: &pb.DeleteRefsRequest{
				ExceptWithPrefix: [][]byte{[]byte("refs/keep"), []byte("refs/also-keep"), []byte("refs/heads/")},
			},
		},
		{
			desc: "delete certain refs",
			request: &pb.DeleteRefsRequest{
				Refs: [][]byte{[]byte("refs/delete/a"), []byte("refs/also-delete/b")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			repo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/delete/a", "b83d6e391c22777fca1ed3012fce84f633d7fed0")
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/also-delete/b", "1b12f15a11fc6e62177bef08f47bc7b5ce50b141")
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/keep/c", "498214de67004b1da3d820901307bed2a68a8ef6")
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/also-keep/d", "b83d6e391c22777fca1ed3012fce84f633d7fed0")

			ctx, cancel := testhelper.Context()
			defer cancel()

			testCase.request.Repository = repo
			_, err := client.DeleteRefs(ctx, testCase.request)
			require.NoError(t, err)

			refs := testhelper.GetRepositoryRefs(t, repoPath)

			require.NotContains(t, refs, "refs/delete/a")
			require.NotContains(t, refs, "refs/also-delete/b")
			require.Contains(t, refs, "refs/keep/c")
			require.Contains(t, refs, "refs/also-keep/d")
			require.Contains(t, refs, "refs/heads/master")
		})
	}
}

func TestFailedDeleteRefsRequestDueToGitError(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &pb.DeleteRefsRequest{
		Repository: repo,
		Refs:       [][]byte{[]byte(`refs\tails\invalid-ref-format`)},
	}

	response, err := client.DeleteRefs(ctx, request)
	require.NoError(t, err)

	require.Contains(t, response.GitError, "Could not delete refs")
}

func TestFailedDeleteRefsDueToValidation(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *pb.DeleteRefsRequest
		// repo     *pb.Repository
		// prefixes [][]byte
		code codes.Code
	}{
		{
			desc: "Invalid repository",
			request: &pb.DeleteRefsRequest{
				Repository:       &pb.Repository{StorageName: "fake", RelativePath: "path"},
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Repository is nil",
			request: &pb.DeleteRefsRequest{
				Repository:       nil,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "No prefixes nor refs",
			request: &pb.DeleteRefsRequest{
				Repository: testRepo,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "prefixes with refs",
			request: &pb.DeleteRefsRequest{
				Repository:       testRepo,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
				Refs:             [][]byte{[]byte("delete-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Empty prefix",
			request: &pb.DeleteRefsRequest{
				Repository:       testRepo,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this"), []byte{}},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Empty ref",
			request: &pb.DeleteRefsRequest{
				Repository: testRepo,
				Refs:       [][]byte{[]byte("delete-this"), []byte{}},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.DeleteRefs(ctx, tc.request)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}
