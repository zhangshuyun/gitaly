package repository

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
)

func TestCreateBundleFromRefList_success(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// Create a work tree with a HEAD pointing to a commit that is missing. CreateBundle should
	// clean this up before creating the bundle.
	sha := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "gitlab-worktree"), 0o755))

	gittest.Exec(t, cfg, "-C", repoPath, "worktree", "add", "gitlab-worktree/worktree1", sha.String())
	require.NoError(t, os.Chtimes(filepath.Join(repoPath, "gitlab-worktree", "worktree1"), time.Now().Add(-7*time.Hour), time.Now().Add(-7*time.Hour)))

	gittest.Exec(t, cfg, "-C", repoPath, "branch", "-D", "branch")
	require.NoError(t, os.Remove(filepath.Join(repoPath, "objects", sha.String()[0:2], sha.String()[2:])))

	masterOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))

	c, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
		Patterns: [][]byte{
			[]byte("master"),
			[]byte("^master~1"),
		},
	}))
	require.NoError(t, c.CloseSend())

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	bundle, err := os.CreateTemp("", "bundle")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(bundle.Name())) }()

	_, err = io.Copy(bundle, reader)
	require.NoError(t, err)

	require.NoError(t, bundle.Close())

	output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())

	require.Contains(t, string(output), fmt.Sprintf("The bundle contains this ref:\n%s refs/heads/master", masterOID))
}

func TestCreateBundleFromRefList_validations(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	testCases := []struct {
		desc    string
		request *gitalypb.CreateBundleFromRefListRequest
		code    codes.Code
	}{
		{
			desc:    "empty repository",
			request: &gitalypb.CreateBundleFromRefListRequest{Patterns: [][]byte{[]byte("master")}},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.CreateBundleFromRefList(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(testCase.request))
			require.NoError(t, stream.CloseSend())

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
