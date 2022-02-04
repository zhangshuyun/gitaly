package localrepo_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRepo_Path(t *testing.T) {
	t.Run("valid repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		path, err := repo.Path()
		require.NoError(t, err)
		require.Equal(t, repoPath, path)
	})

	t.Run("deleted repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, os.RemoveAll(repoPath))

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})

	t.Run("non-git repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Recreate the repository as a simple empty directory to simulate
		// that the repository is in a partially-created state.
		require.NoError(t, os.RemoveAll(repoPath))
		require.NoError(t, os.MkdirAll(repoPath, 0o777))

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})
}

func TestRepo_ObjectDirectoryPath(t *testing.T) {
	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	locator := config.NewLocator(cfg)

	ctx := testhelper.Context(t)

	quarantine, err := quarantine.New(ctx, repoProto, locator)
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
			repo: repoWithGitObjDir(repoProto, "objects/"),
			path: filepath.Join(repoPath, "objects/"),
		},
		{
			desc: "no GitObjectDirectoryPath",
			repo: repoProto,
			err:  codes.InvalidArgument,
		},
		{
			desc: "with directory traversal",
			repo: repoWithGitObjDir(repoProto, "../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "valid path but doesn't exist",
			repo: repoWithGitObjDir(repoProto, "foo../bazqux.git"),
			err:  codes.NotFound,
		},
		{
			desc: "with sneaky directory traversal",
			repo: repoWithGitObjDir(repoProto, "/../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository",
			repo: repoWithGitObjDir(repoProto, "objects/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository with trailing separator",
			repo: repoWithGitObjDir(repoProto, "objects/../../"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with deep traversal at the end",
			repo: repoWithGitObjDir(repoProto, "bazqux.git/../.."),
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
			repo := localrepo.NewTestRepo(t, cfg, tc.repo)

			path, err := repo.ObjectDirectoryPath()

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
