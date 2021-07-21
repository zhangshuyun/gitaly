package config_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestConfigLocator_GetObjectDirectoryPath(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	locator := config.NewLocator(cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	quarantine, err := quarantine.New(ctx, repo, locator)
	require.NoError(t, err)
	quarantinedRepo := quarantine.QuarantinedRepo()

	repoWithGitObjDir := func(repo *gitalypb.Repository, dir string) *gitalypb.Repository {
		repo = proto.Clone(repo).(*gitalypb.Repository)
		repo.GitObjectDirectory = dir
		return repo
	}

	testCases := []struct {
		desc string
		repo *gitalypb.Repository
		path string
		err  codes.Code
	}{
		{
			desc: "storages configured",
			repo: repoWithGitObjDir(repo, "objects/"),
			path: filepath.Join(repoPath, "objects/"),
		},
		{
			desc: "no GitObjectDirectoryPath",
			repo: repo,
			err:  codes.InvalidArgument,
		},
		{
			desc: "with directory traversal",
			repo: repoWithGitObjDir(repo, "../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "valid path but doesn't exist",
			repo: repoWithGitObjDir(repo, "foo../bazqux.git"),
			err:  codes.NotFound,
		},
		{
			desc: "with sneaky directory traversal",
			repo: repoWithGitObjDir(repo, "/../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository",
			repo: repoWithGitObjDir(repo, "objects/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository with trailing separator",
			repo: repoWithGitObjDir(repo, "objects/../../"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with deep traversal at the end",
			repo: repoWithGitObjDir(repo, "bazqux.git/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "quarantined repo",
			repo: quarantinedRepo,
			path: filepath.Join(repoPath, quarantinedRepo.GetGitObjectDirectory()),
		},
		{
			desc: "quarantined repo with parent directory",
			repo: repoWithGitObjDir(quarantinedRepo, quarantinedRepo.GetGitObjectDirectory()+"/.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "quarantined repo with directory traversal",
			repo: repoWithGitObjDir(quarantinedRepo, quarantinedRepo.GetGitObjectDirectory()+"/../foobar.git"),
			err:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			path, err := locator.GetObjectDirectoryPath(tc.repo)

			if tc.err != codes.OK {
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tc.err, st.Code())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.path, path)
		})
	}
}
