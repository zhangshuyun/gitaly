package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestSuccessfulFindLicenseRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	req := &gitalypb.FindLicenseRequest{Repository: testRepo}

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.FindLicense(ctx, req)

	require.NoError(t, err)
	require.Equal(t, "mit", resp.GetLicenseShortName())
}

func TestFindLicenseRequestEmptyRepo(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	emptyRepo := &gitalypb.Repository{
		RelativePath: "test-liceense-empty-repo.git",
		StorageName:  testhelper.DefaultStorageName,
	}

	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: emptyRepo})
	require.NoError(t, err)

	emptyRepoPath, err := helper.GetRepoPath(emptyRepo)
	require.NoError(t, err)
	defer os.RemoveAll(emptyRepoPath)

	req := &gitalypb.FindLicenseRequest{Repository: emptyRepo}

	resp, err := client.FindLicense(ctx, req)
	require.NoError(t, err)

	require.Empty(t, resp.GetLicenseShortName())
}
