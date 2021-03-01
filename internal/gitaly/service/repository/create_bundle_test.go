package repository

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateBundleRequest(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	// create a work tree with a HEAD pointing to a commit that is missing.
	// CreateBundle should clean this up before creating the bundle
	sha, branchName := gittest.CreateCommitOnNewBranch(t, testRepoPath)

	require.NoError(t, os.MkdirAll(filepath.Join(testRepoPath, "gitlab-worktree"), 0755))

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "worktree", "add", "gitlab-worktree/worktree1", sha)
	os.Chtimes(filepath.Join(testRepoPath, "gitlab-worktree", "worktree1"), time.Now().Add(-7*time.Hour), time.Now().Add(-7*time.Hour))

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-D", branchName)
	require.NoError(t, os.Remove(filepath.Join(testRepoPath, "objects", sha[0:2], sha[2:])))

	request := &gitalypb.CreateBundleRequest{Repository: testRepo}

	c, err := client.CreateBundle(ctx, request)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	dstDir, err := tempdir.New(ctx, testRepo, locator)
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
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
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
