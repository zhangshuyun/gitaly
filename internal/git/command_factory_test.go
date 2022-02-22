package git_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestGitCommandProxy(t *testing.T) {
	cfg := testcfg.Build(t)

	requestReceived := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
	}))
	defer ts.Close()

	oldHTTPProxy := os.Getenv("http_proxy")
	defer require.NoError(t, os.Setenv("http_proxy", oldHTTPProxy))
	require.NoError(t, os.Setenv("http_proxy", ts.URL))
	ctx := testhelper.Context(t)

	dir := testhelper.TempDir(t)

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	cmd, err := gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
		Name: "clone",
		Args: []string{"http://gitlab.com/bogus-repo", dir},
	}, git.WithDisabledHooks())
	require.NoError(t, err)

	err = cmd.Wait()
	require.NoError(t, err)
	require.True(t, requestReceived)
}

// Global git configuration is only disabled in tests for now. Gitaly should stop using the global
// git configuration in 15.0. See https://gitlab.com/gitlab-org/gitaly/-/issues/3617.
func TestExecCommandFactory_globalGitConfigIgnored(t *testing.T) {
	cfg := testcfg.Build(t)

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	tmpHome := testhelper.TempDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(tmpHome, ".gitconfig"), []byte(`[ignored]
	value = true
`,
	), os.ModePerm))
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc   string
		filter string
	}{
		{desc: "global", filter: "--global"},
		// The test doesn't override the system config as that would be a global change or would
		// require chrooting. The assertion won't catch problems on systems that do not have system
		// level configuration set.
		{desc: "system", filter: "--system"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd, err := gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
				Name:  "config",
				Flags: []git.Option{git.Flag{Name: "--list"}, git.Flag{Name: tc.filter}},
			}, git.WithEnv("HOME="+tmpHome))
			require.NoError(t, err)

			configContents, err := io.ReadAll(cmd)
			require.NoError(t, err)
			require.NoError(t, cmd.Wait())
			require.Empty(t, string(configContents))
		})
	}
}

func TestExecCommandFactory_NewWithDir(t *testing.T) {
	cfg := testcfg.Build(t)

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	t.Run("no dir specified", func(t *testing.T) {
		ctx := testhelper.Context(t)

		_, err := gitCmdFactory.NewWithDir(ctx, "", nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no 'dir' provided")
	})

	t.Run("runs in dir", func(t *testing.T) {
		repoPath := testhelper.TempDir(t)

		gittest.Exec(t, cfg, "init", repoPath)
		gittest.Exec(t, cfg, "-C", repoPath, "commit", "--allow-empty", "-m", "initial commit")
		ctx := testhelper.Context(t)

		var stderr bytes.Buffer
		cmd, err := gitCmdFactory.NewWithDir(ctx, repoPath, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		}, git.WithStderr(&stderr))
		require.NoError(t, err)

		revData, err := io.ReadAll(cmd)
		require.NoError(t, err)

		require.NoError(t, cmd.Wait(), stderr.String())

		require.Equal(t, "99ed180822d96f70810847eba6d0d168c582258d", text.ChompBytes(revData))
	})

	t.Run("doesn't runs in non existing dir", func(t *testing.T) {
		ctx := testhelper.Context(t)

		var stderr bytes.Buffer
		_, err := gitCmdFactory.NewWithDir(ctx, "non-existing-dir", git.SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		}, git.WithStderr(&stderr))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})
}

func TestCommandFactory_ExecutionEnvironment(t *testing.T) {
	testhelper.ModifyEnvironment(t, "GITALY_TESTING_GIT_BINARY", "")
	testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", "")

	ctx := testhelper.Context(t)

	assertExecEnv := func(t *testing.T, cfg config.Cfg, expectedExecEnv git.ExecutionEnvironment) {
		t.Helper()
		gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, git.WithSkipHooks())
		require.NoError(t, err)
		defer cleanup()
		require.Equal(t, expectedExecEnv, gitCmdFactory.GetExecutionEnvironment(ctx))
	}

	t.Run("set in config", func(t *testing.T) {
		assertExecEnv(t, config.Cfg{
			Git: config.Git{BinPath: "/path/to/myGit"},
		}, git.ExecutionEnvironment{
			BinaryPath: "/path/to/myGit",
		})
	})

	t.Run("set using GITALY_TESTING_GIT_BINARY", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_GIT_BINARY", "/path/to/env_git")

		assertExecEnv(t, config.Cfg{Git: config.Git{}}, git.ExecutionEnvironment{
			BinaryPath: "/path/to/env_git",
		})
	})

	t.Run("set using GITALY_TESTING_BUNDLED_GIT_PATH", func(t *testing.T) {
		bundledGitDir := testhelper.TempDir(t)
		bundledGitExecutable := filepath.Join(bundledGitDir, "gitaly-git")
		bundledGitRemoteExecutable := filepath.Join(bundledGitDir, "gitaly-git-remote-http")
		bundledGitHTTPBackendExecutable := filepath.Join(bundledGitDir, "gitaly-git-http-backend")

		testhelper.ModifyEnvironment(t, "GITALY_TESTING_BUNDLED_GIT_PATH", bundledGitDir)

		t.Run("missing bin_dir", func(t *testing.T) {
			_, _, err := git.NewExecCommandFactory(config.Cfg{Git: config.Git{}}, git.WithSkipHooks())
			require.EqualError(t, err, "setting up Git execution environment: constructing bundled Git environment: cannot use bundled binaries without bin path being set")
		})

		t.Run("missing gitaly-git executable", func(t *testing.T) {
			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), `statting "gitaly-git":`)
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("missing git-remote-http executable", func(t *testing.T) {
			require.NoError(t, os.WriteFile(bundledGitExecutable, []byte{}, 0o777))

			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), "statting \"gitaly-git-remote-http\":")
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("missing git-http-backend executable", func(t *testing.T) {
			require.NoError(t, os.WriteFile(bundledGitExecutable, []byte{}, 0o777))
			require.NoError(t, os.WriteFile(bundledGitRemoteExecutable, []byte{}, 0o777))

			_, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: testhelper.TempDir(t)}, git.WithSkipHooks())
			require.Error(t, err)
			require.Contains(t, err.Error(), "statting \"gitaly-git-http-backend\":")
			require.True(t, errors.Is(err, os.ErrNotExist))
		})

		t.Run("bin_dir with executables", func(t *testing.T) {
			require.NoError(t, os.WriteFile(bundledGitExecutable, []byte{}, 0o777))
			require.NoError(t, os.WriteFile(bundledGitRemoteExecutable, []byte{}, 0o777))
			require.NoError(t, os.WriteFile(bundledGitHTTPBackendExecutable, []byte{}, 0o777))

			binDir := testhelper.TempDir(t)

			gitCmdFactory, _, err := git.NewExecCommandFactory(config.Cfg{BinDir: binDir}, git.WithSkipHooks())
			require.NoError(t, err)

			gitBinPath := gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath

			for _, executable := range []string{"git", "git-remote-http", "git-http-backend"} {
				symlinkPath := filepath.Join(filepath.Dir(gitBinPath), executable)

				// The symlink in Git's temporary BinPath points to the Git
				// executable in Gitaly's BinDir.
				target, err := os.Readlink(symlinkPath)
				require.NoError(t, err)
				require.Equal(t, filepath.Join(binDir, "gitaly-"+executable), target)

				// And in a test setup, the symlink in Gitaly's BinDir must point to
				// the Git binary pointed to by the GITALY_TESTING_BUNDLED_GIT_PATH
				// environment variable.
				target, err = os.Readlink(target)
				require.NoError(t, err)
				require.Equal(t, filepath.Join(bundledGitDir, "gitaly-"+executable), target)
			}
		})
	})

	t.Run("not set, get from system", func(t *testing.T) {
		resolvedPath, err := exec.LookPath("git")
		require.NoError(t, err)

		assertExecEnv(t, config.Cfg{}, git.ExecutionEnvironment{
			BinaryPath: resolvedPath,
		})
	})

	t.Run("doesn't exist in the system", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "PATH", "")

		_, _, err := git.NewExecCommandFactory(config.Cfg{}, git.WithSkipHooks())
		require.EqualError(t, err, `setting up Git execution environment: "git" executable not found, set path to it in the configuration file or add it to the PATH`)
	})
}

func TestExecCommandFactoryHooksPath(t *testing.T) {
	ctx := testhelper.Context(t)

	t.Run("temporary hooks", func(t *testing.T) {
		cfg := config.Cfg{
			BinDir: testhelper.TempDir(t),
		}

		t.Run("no overrides", func(t *testing.T) {
			gitCmdFactory := gittest.NewCommandFactory(t, cfg)

			hooksPath := gitCmdFactory.HooksPath(ctx)

			// We cannot assert that the hooks path is equal to any specific
			// string, but instead we can assert that it exists and contains the
			// symlinks we expect.
			for _, hook := range []string{"update", "pre-receive", "post-receive", "reference-transaction"} {
				target, err := os.Readlink(filepath.Join(hooksPath, hook))
				require.NoError(t, err)
				require.Equal(t, filepath.Join(cfg.BinDir, "gitaly-hooks"), target)
			}
		})

		t.Run("with skip", func(t *testing.T) {
			gitCmdFactory := gittest.NewCommandFactory(t, cfg, git.WithSkipHooks())
			require.Equal(t, "/var/empty", gitCmdFactory.HooksPath(ctx))
		})
	})

	t.Run("hooks path", func(t *testing.T) {
		gitCmdFactory := gittest.NewCommandFactory(t, config.Cfg{
			BinDir: testhelper.TempDir(t),
		}, git.WithHooksPath("/hooks/path"))

		// The environment variable shouldn't override an explicitly set hooks path.
		require.Equal(t, "/hooks/path", gitCmdFactory.HooksPath(ctx))
	})
}

func TestExecCommandFactory_GitVersion(t *testing.T) {
	ctx := testhelper.Context(t)

	generateVersionScript := func(version string) func(git.ExecutionEnvironment) string {
		return func(git.ExecutionEnvironment) string {
			return fmt.Sprintf(
				`#!/usr/bin/env bash
				echo '%s'
			`, version)
		}
	}

	for _, tc := range []struct {
		desc            string
		versionString   string
		expectedErr     string
		expectedVersion string
	}{
		{
			desc:            "valid version",
			versionString:   "git version 2.33.1.gl1",
			expectedVersion: "2.33.1.gl1",
		},
		{
			desc:            "valid version with trailing newline",
			versionString:   "git version 2.33.1.gl1\n",
			expectedVersion: "2.33.1.gl1",
		},
		{
			desc:          "multi-line version",
			versionString: "git version 2.33.1.gl1\nfoobar\n",
			expectedErr:   "cannot parse git version: strconv.ParseUint: parsing \"1\\nfoobar\": invalid syntax",
		},
		{
			desc:          "unexpected format",
			versionString: "2.33.1\n",
			expectedErr:   "invalid version format: \"2.33.1\\n\\n\"",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			gitCmdFactory := gittest.NewInterceptingCommandFactory(
				ctx, t, testcfg.Build(t), generateVersionScript(tc.versionString), git.WithSkipHooks(),
			)

			actualVersion, err := gitCmdFactory.GitVersion(ctx)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedVersion, actualVersion.String())
		})
	}

	t.Run("caching", func(t *testing.T) {
		gitCmdFactory := gittest.NewInterceptingCommandFactory(
			ctx, t, testcfg.Build(t), generateVersionScript("git version 1.2.3"), git.WithSkipHooks(),
		)

		gitPath := gitCmdFactory.GetExecutionEnvironment(ctx).BinaryPath
		stat, err := os.Stat(gitPath)
		require.NoError(t, err)

		version, err := gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())

		// We rewrite the file with the same content length and modification time such that
		// its file information doesn't change. As a result, information returned by
		// stat(3P) shouldn't differ and we should continue to see the cached version. This
		// is a known insufficiency, but it is extremely unlikely to ever happen in
		// production when the real Git binary changes.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(generateVersionScript("git version 9.8.7")(git.ExecutionEnvironment{})))
		require.NoError(t, os.Chtimes(gitPath, stat.ModTime(), stat.ModTime()))

		// Given that we continue to use the cached version we shouldn't see any
		// change here.
		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())

		// If we really replace the Git binary with something else, then we should
		// see a changed version.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(
			`#!/usr/bin/env bash
			echo 'git version 2.34.1'
		`))

		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "2.34.1", version.String())
	})
}
