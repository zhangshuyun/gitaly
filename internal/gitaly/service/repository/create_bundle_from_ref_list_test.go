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

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

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

	bundle, err := os.Create(filepath.Join(testhelper.TempDir(t), "bundle"))
	require.NoError(t, err)

	_, err = io.Copy(bundle, reader)
	require.NoError(t, err)

	require.NoError(t, bundle.Close())

	output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())

	require.Contains(t, string(output), fmt.Sprintf("The bundle contains this ref:\n%s refs/heads/master", masterOID))
}

func TestCreateBundleFromRefList_missing_ref(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

	masterOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))

	c, err := client.CreateBundleFromRefList(ctx)
	require.NoError(t, err)

	require.NoError(t, c.Send(&gitalypb.CreateBundleFromRefListRequest{
		Repository: repo,
		Patterns: [][]byte{
			[]byte("refs/heads/master"),
			[]byte("refs/heads/totally_missing"),
		},
	}))
	require.NoError(t, c.CloseSend())

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	bundle, err := os.Create(filepath.Join(testhelper.TempDir(t), "bundle"))
	require.NoError(t, err)

	_, err = io.Copy(bundle, reader)
	require.NoError(t, err)

	require.NoError(t, bundle.Close())

	output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundle.Name())

	require.Contains(t, string(output), fmt.Sprintf("The bundle contains this ref:\n%s refs/heads/master", masterOID))
}

func TestCreateBundleFromRefList_validations(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	testCases := []struct {
		desc         string
		request      *gitalypb.CreateBundleFromRefListRequest
		expectedErr  string
		expectedCode codes.Code
	}{
		{
			desc: "empty repository",
			request: &gitalypb.CreateBundleFromRefListRequest{
				Patterns: [][]byte{[]byte("master")},
			},
			expectedErr:  "empty Repository",
			expectedCode: codes.InvalidArgument,
		},
		{
			desc: "empty bundle",
			request: &gitalypb.CreateBundleFromRefListRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("master"), []byte("^master")},
			},
			expectedErr:  "cmd wait failed: refusing to create empty bundle",
			expectedCode: codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.CreateBundleFromRefList(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(testCase.request))
			require.NoError(t, stream.CloseSend())

			for {
				_, err = stream.Recv()
				if err != nil {
					break
				}
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.expectedErr)
			testhelper.RequireGrpcCode(t, err, testCase.expectedCode)
		})
	}
}
