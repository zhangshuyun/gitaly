package repository

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCalculateChecksum(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRepositoryService(t)

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoPath, d), 0o755))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-packed-refs", filepath.Join(repoPath, "packed-refs")).Run())
	require.NoError(t, exec.Command(cfg.Git.BinPath, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/feature").Run())

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "0c500d7c8a9dbf65e4cf5e58914bec45bfb6e9ab", response.Checksum)
}

func TestEmptyRepositoryCalculateChecksum(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, git.ZeroOID.String(), response.Checksum)
}

func TestBrokenRepositoryCalculateChecksum(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

	// Force an empty HEAD file
	require.NoError(t, os.Truncate(filepath.Join(repoPath, "HEAD"), 0))

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	_, err := client.CalculateChecksum(testCtx, request)
	testhelper.RequireGrpcError(t, err, codes.DataLoss)
}

func TestFailedCalculateChecksum(t *testing.T) {
	t.Parallel()
	_, client := setupRepositoryServiceWithoutRepo(t)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.CalculateChecksumRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.CalculateChecksumRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.CalculateChecksumRequest{},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		testCtx, cancelCtx := testhelper.Context()
		defer cancelCtx()

		_, err := client.CalculateChecksum(testCtx, testCase.request)
		testhelper.RequireGrpcError(t, err, testCase.code)
	}
}

func TestInvalidRefsCalculateChecksum(t *testing.T) {
	t.Parallel()
	_, repo, repoPath, client := setupRepositoryService(t)

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoPath, d), 0o755))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-invalid-refs", filepath.Join(repoPath, "packed-refs")).Run())

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, git.ZeroOID.String(), response.Checksum)
}
