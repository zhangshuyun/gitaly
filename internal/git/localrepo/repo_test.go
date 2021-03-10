package localrepo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepo(t *testing.T) {
	cfg, cleanup := testcfg.Build(t)
	defer cleanup()

	gittest.TestRepository(t, cfg, func(t testing.TB, pbRepo *gitalypb.Repository) git.Repository {
		t.Helper()
		return New(git.NewExecCommandFactory(cfg), pbRepo, cfg)
	})
}

func TestRepo_Path(t *testing.T) {
	t.Run("valid repository", func(t *testing.T) {
		cfg, repoProto, repoPath, cleanup := testcfg.BuildWithRepo(t)
		defer cleanup()
		repo := New(git.NewExecCommandFactory(cfg), repoProto, cfg)

		path, err := repo.Path()
		require.NoError(t, err)
		require.Equal(t, repoPath, path)
	})

	t.Run("deleted repository", func(t *testing.T) {
		cfg, repoProto, _, cleanup := testcfg.BuildWithRepo(t)
		defer cleanup()
		repo := New(git.NewExecCommandFactory(cfg), repoProto, cfg)

		cleanup()

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})

	t.Run("non-git repository", func(t *testing.T) {
		cfg, repoProto, repoPath, cleanup := testcfg.BuildWithRepo(t)
		defer cleanup()
		repo := New(git.NewExecCommandFactory(cfg), repoProto, cfg)

		// Recreate the repository as a simple empty directory to simulate
		// that the repository is in a partially-created state.
		require.NoError(t, os.RemoveAll(repoPath))
		require.NoError(t, os.MkdirAll(repoPath, 0777))

		_, err := repo.Path()
		require.Equal(t, codes.NotFound, helper.GrpcCode(err))
	})
}
