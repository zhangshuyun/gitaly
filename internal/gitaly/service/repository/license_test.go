package repository

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func testSuccessfulFindLicenseRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFindLicense,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		_, repo, _, client := setupRepositoryWithWorkingtreeServiceWithRuby(t, cfg, rubySrv)

		resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})

		require.NoError(t, err)
		require.Equal(t, "mit", resp.GetLicenseShortName())
	})
}

func testFindLicenseRequestEmptyRepo(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFindLicense,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		cfg, _, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

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
	})
}
