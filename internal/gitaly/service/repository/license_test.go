package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func testSuccessfulFindLicenseRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	_, repo, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})

	require.NoError(t, err)
	require.Equal(t, "mit", resp.GetLicenseShortName())
}

func testFindLicenseRequestEmptyRepo(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, _, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	ctx, cancel := testhelper.Context()
	defer cancel()

	emptyRepo := &gitalypb.Repository{
		RelativePath: "test-liceense-empty-repo.git",
		StorageName:  cfg.Storages[0].Name,
	}
	emptyRepoPath := filepath.Join(cfg.Storages[0].Path, emptyRepo.GetRelativePath())
	defer os.RemoveAll(emptyRepoPath)

	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: emptyRepo})
	require.NoError(t, err)

	resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: emptyRepo})
	require.NoError(t, err)

	require.Empty(t, resp.GetLicenseShortName())
}
