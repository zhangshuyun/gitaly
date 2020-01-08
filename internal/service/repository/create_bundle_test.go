package repository

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateBundleRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.CreateBundleRequest{Repository: testRepo}

	c, err := client.CreateBundle(ctx, request)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	dstDir, err := tempdir.New(ctx, testRepo)
	require.NoError(t, err)
	dstFile, err := ioutil.TempFile(dstDir, "")
	require.NoError(t, err)
	defer dstFile.Close()
	defer os.RemoveAll(dstFile.Name())

	_, err = io.Copy(dstFile, reader)
	require.NoError(t, err)

	output := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "bundle", "verify", dstFile.Name())
	// Extra sanity; running verify should fail on bad bundles
	require.Contains(t, string(output), "The bundle records a complete history")
}

func TestFailedCreateBundleRequestDueToValidations(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *gitalypb.CreateBundleRequest
		code    codes.Code
	}{
		{
			desc:    "empty repository",
			request: &gitalypb.CreateBundleRequest{},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.CreateBundle(ctx, testCase.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
