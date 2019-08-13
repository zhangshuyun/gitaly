package diff

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitPatchRequest(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		revision []byte
		diff     []byte
	}{
		{
			desc:     "With a commit id",
			revision: []byte("2f63565e7aac07bcdadb654e253078b727143ec4"),
			diff:     testhelper.MustReadFile(t, "testdata/binary-changes-patch.txt"),
		},
		{
			desc:     "With a root commit id",
			revision: []byte("1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"),
			diff:     testhelper.MustReadFile(t, "testdata/initial-commit-patch.txt"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.CommitPatchRequest{
				Repository: testRepo,
				Revision:   testCase.revision,
			}

			c, err := client.CommitPatch(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			data := []byte{}
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}

				data = append(data, r.GetData()...)
			}

			assert.Equal(t, testCase.diff, data)
		})
	}
}

func TestInvalidCommitPatchRequestRevision(t *testing.T) {
	server, serverSocketPath := runDiffServer(t)
	defer server.Stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CommitPatch(ctx, &gitalypb.CommitPatchRequest{
		Repository: testRepo,
		Revision:   []byte("--output=/meow"),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
