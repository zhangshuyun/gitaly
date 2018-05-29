package repository

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindFindMergeBaseRequest(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc      string
		revisions [][]byte
		base      string
	}{
		{
			desc: "oid revisions",
			revisions: [][]byte{
				[]byte("372ab6950519549b14d220271ee2322caa44d4eb"),
				[]byte("8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"),
			},
			base: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
		},
		{
			desc: "branch revisions",
			revisions: [][]byte{
				[]byte("master"),
				[]byte("gitaly-stuff"),
			},
			base: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
		},
		{
			desc: "non-existent merge base",
			revisions: [][]byte{
				[]byte("master"),
				[]byte("orphaned-branch"),
			},
			base: "",
		},
		{
			desc: "non-existent branch",
			revisions: [][]byte{
				[]byte("master"),
				[]byte("a-branch-that-does-not-exist"),
			},
			base: "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.FindMergeBaseRequest{
				Repository: testRepo,
				Revisions:  testCase.revisions,
			}

			response, err := client.FindMergeBase(ctx, request)
			require.NoError(t, err)

			require.Equal(t, testCase.base, response.Base)
		})
	}
}

func TestFailedFindMergeBaseRequestDueToValidations(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc      string
		revisions [][]byte
		code      codes.Code
	}{
		{
			desc: "1 revision",
			revisions: [][]byte{
				[]byte("372ab6950519549b14d220271ee2322caa44d4eb"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "2+ revisions",
			revisions: [][]byte{
				[]byte("master"),
				[]byte("gitaly-stuff"),
				[]byte("spooky-stuff"),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.FindMergeBaseRequest{
				Repository: testRepo,
				Revisions:  testCase.revisions,
			}

			_, err := client.FindMergeBase(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
