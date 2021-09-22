package localrepo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepo(t *testing.T) {
	cfg := testcfg.Build(t)

	gittest.TestRepository(t, cfg, func(t testing.TB, pbRepo *gitalypb.Repository) git.Repository {
		t.Helper()
		gitCmdFactory := git.NewExecCommandFactory(cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		return New(gitCmdFactory, catfileCache, pbRepo, cfg)
	})
}

func TestRepo_Path(t *testing.T) {
	t.Run("valid repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		gitCmdFactory := git.NewExecCommandFactory(cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		repo := New(gitCmdFactory, catfileCache, repoProto, cfg)

		path, err := repo.Path()
		require.NoError(t, err)
		require.Equal(t, repoPath, path)
	})

	t.Run("deleted repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		gitCmdFactory := git.NewExecCommandFactory(cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		repo := New(gitCmdFactory, catfileCache, repoProto, cfg)

		require.NoError(t, os.RemoveAll(repoPath))

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})

	t.Run("non-git repository", func(t *testing.T) {
		cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
		gitCmdFactory := git.NewExecCommandFactory(cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		repo := New(gitCmdFactory, catfileCache, repoProto, cfg)

		// Recreate the repository as a simple empty directory to simulate
		// that the repository is in a partially-created state.
		require.NoError(t, os.RemoveAll(repoPath))
		require.NoError(t, os.MkdirAll(repoPath, 0o777))

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})
}
