package git_test

import (
	"errors"
	"os"
	"path/filepath"
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

func TestBundledGitEnvironmentConstructor(t *testing.T) {
	testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", "")

	constructor := git.BundledGitEnvironmentConstructor{}

	seedDirWithExecutables := func(t *testing.T, executableNames ...string) string {
		dir := testhelper.TempDir(t)
		for _, executableName := range executableNames {
			require.NoError(t, os.WriteFile(filepath.Join(dir, executableName), nil, 0o777))
		}
		return dir
	}

	t.Run("disabled bundled Git fails", func(t *testing.T) {
		_, _, err := constructor.Construct(config.Cfg{})
		require.Equal(t, git.ErrNotConfigured, err)
	})

	t.Run("bundled Git without binary directory fails", func(t *testing.T) {
		_, cleanup, err := constructor.Construct(config.Cfg{
			Git: config.Git{
				UseBundledBinaries: true,
			},
		})

		// It is a bug that this succeeds: if the binary directory is not set we cannot
		// derive the location of the bundled Git executables either.
		require.NoError(t, err)
		defer cleanup()
	})

	t.Run("incomplete binary directory succeeds", func(t *testing.T) {
		_, cleanup, err := constructor.Construct(config.Cfg{
			BinDir: seedDirWithExecutables(t, "gitaly-git", "gitaly-git-remote-http"),
			Git: config.Git{
				UseBundledBinaries: true,
			},
		})

		// It is a bug that this succeeds, we really should check that all expected binaries
		// exist. We thus don't bother to check the generated execution environment.
		require.NoError(t, err)
		defer cleanup()
	})

	t.Run("complete binary directory succeeds", func(t *testing.T) {
		binDir := seedDirWithExecutables(t, "gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend")

		execEnv, cleanup, err := constructor.Construct(config.Cfg{
			BinDir: binDir,
			Git: config.Git{
				UseBundledBinaries: true,
			},
		})
		require.NoError(t, err)
		defer cleanup()

		// We create a temporary directory where the symlinks are created, and we cannot
		// predict its exact path.
		require.Equal(t, "git", filepath.Base(execEnv.BinaryPath))

		execPrefix := filepath.Dir(execEnv.BinaryPath)
		require.Equal(t, []string{
			"GIT_EXEC_PATH=" + execPrefix,
		}, execEnv.EnvironmentVariables)

		for _, binary := range []string{"git", "git-remote-http", "git-http-backend"} {
			target, err := filepath.EvalSymlinks(filepath.Join(execPrefix, binary))
			require.NoError(t, err)
			require.Equal(t, filepath.Join(binDir, "gitaly-"+binary), target)
		}
	})

	t.Run("cleanup removes temporary directory", func(t *testing.T) {
		execEnv, cleanup, err := constructor.Construct(config.Cfg{
			BinDir: seedDirWithExecutables(t, "gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend"),
			Git: config.Git{
				UseBundledBinaries: true,
			},
		})
		require.NoError(t, err)

		execPrefix := filepath.Dir(execEnv.BinaryPath)
		require.DirExists(t, execPrefix)

		cleanup()

		require.NoDirExists(t, execPrefix)
	})

	t.Run("bundled Git path without binary directory fails", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", "/does/not/exist")
		_, _, err := constructor.Construct(config.Cfg{})
		require.Equal(t, errors.New("cannot use bundled binaries without bin path being set"), err)
	})

	t.Run("nonexistent bundled Git path via environment fails", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", "/does/not/exist")
		_, _, err := constructor.Construct(config.Cfg{
			BinDir: testhelper.TempDir(t),
		})
		require.Error(t, err)
		require.Equal(t, err.Error(), "statting \"gitaly-git\": stat /does/not/exist/gitaly-git: no such file or directory")
	})

	t.Run("incomplete bundled Git environment fails", func(t *testing.T) {
		bundledGitPath := seedDirWithExecutables(t, "gitaly-git", "gitaly-git-remote-http")
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", bundledGitPath)

		_, _, err := constructor.Construct(config.Cfg{
			BinDir: testhelper.TempDir(t),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "statting \"gitaly-git-http-backend\": ")
	})

	t.Run("complete bundled Git environment populates binary directory", func(t *testing.T) {
		bundledGitPath := seedDirWithExecutables(t, "gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend")
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", bundledGitPath)

		execEnv, cleanup, err := constructor.Construct(config.Cfg{
			BinDir: testhelper.TempDir(t),
		})
		require.NoError(t, err)
		defer cleanup()

		require.Equal(t, "git", filepath.Base(execEnv.BinaryPath))
		execPrefix := filepath.Dir(execEnv.BinaryPath)

		require.Equal(t, []string{
			"GIT_EXEC_PATH=" + execPrefix,
		}, execEnv.EnvironmentVariables)

		for _, binary := range []string{"git", "git-remote-http", "git-http-backend"} {
			target, err := filepath.EvalSymlinks(filepath.Join(execPrefix, binary))
			require.NoError(t, err)
			require.Equal(t, filepath.Join(bundledGitPath, "gitaly-"+binary), target)
		}
	})
}
