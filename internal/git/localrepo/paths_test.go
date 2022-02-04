package localrepo_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"google.golang.org/grpc/codes"
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
