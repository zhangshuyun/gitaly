package git_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestDistributedGitEnvironmentConstructor(t *testing.T) {
	constructor := git.DistributedGitEnvironmentConstructor{}

	testhelper.ModifyEnvironment(t, "GITALY_TESTING_GIT_BINARY", "")

	t.Run("empty configuration fails", func(t *testing.T) {
		_, _, err := constructor.Construct(config.Cfg{})
		require.Equal(t, git.ErrNotConfigured, err)
	})

	t.Run("configuration with Git binary path succeeds", func(t *testing.T) {
		execEnv, cleanup, err := constructor.Construct(config.Cfg{
			Git: config.Git{
				BinPath: "/foo/bar",
			},
		})
		require.NoError(t, err)
		defer cleanup()

		require.Equal(t, "/foo/bar", execEnv.BinaryPath)
		require.Equal(t, []string(nil), execEnv.EnvironmentVariables)
	})

	t.Run("empty configuration with environment override", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_GIT_BINARY", "/foo/bar")

		execEnv, cleanup, err := constructor.Construct(config.Cfg{})
		require.NoError(t, err)
		defer cleanup()

		require.Equal(t, "/foo/bar", execEnv.BinaryPath)
		require.Equal(t, []string(nil), execEnv.EnvironmentVariables)
	})

	t.Run("configuration overrides environment variable", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_GIT_BINARY", "envvar")

		execEnv, cleanup, err := constructor.Construct(config.Cfg{
			Git: config.Git{
				BinPath: "config",
			},
		})
		require.NoError(t, err)
		defer cleanup()

		require.Equal(t, "config", execEnv.BinaryPath)
		require.Equal(t, []string(nil), execEnv.EnvironmentVariables)
	})
}
