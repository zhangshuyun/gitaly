package git_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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

	ctx, cancel := testhelper.Context()
	defer cancel()

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

	ctx, cancel := testhelper.Context()
	defer cancel()

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
		ctx, cancel := testhelper.Context()
		defer cancel()

		_, err := gitCmdFactory.NewWithDir(ctx, "", nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no 'dir' provided")
	})

	t.Run("runs in dir", func(t *testing.T) {
		repoPath := testhelper.TempDir(t)

		gittest.Exec(t, cfg, "init", repoPath)
		gittest.Exec(t, cfg, "-C", repoPath, "commit", "--allow-empty", "-m", "initial commit")

		ctx, cancel := testhelper.Context()
		defer cancel()

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
		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		_, err := gitCmdFactory.NewWithDir(ctx, "non-existing-dir", git.SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		}, git.WithStderr(&stderr))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})
}

func TestExecCommandFactory_GetExecutionEnvironment(t *testing.T) {
	cfg := testcfg.Build(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	require.NoError(t, err)
	defer cleanup()

	require.Equal(t, git.ExecutionEnvironment{
		BinaryPath:           cfg.Git.BinPath,
		EnvironmentVariables: cfg.GitExecEnv(),
	}, gitCmdFactory.GetExecutionEnvironment(ctx))
}

func TestExecCommandFatcory_HooksPath(t *testing.T) {
	hookDir := setupTempHookDirs(t, map[string]hookFileMode{
		"ruby/git-hooks/update":       hookFileExists | hookFileExecutable,
		"ruby/git-hooks/pre-receive":  hookFileExists | hookFileExecutable,
		"ruby/git-hooks/post-receive": hookFileExists | hookFileExecutable,
	})
	rubyDir := filepath.Join(hookDir, "ruby")

	t.Run("Ruby directory", func(t *testing.T) {
		gitCmdFactory := gittest.NewCommandFactory(t, config.Cfg{
			Ruby: config.Ruby{
				Dir: rubyDir,
			},
		})

		t.Run("no overrides", func(t *testing.T) {
			require.Equal(t, filepath.Join(rubyDir, "git-hooks"), gitCmdFactory.HooksPath())
		})

		t.Run("with skip", func(t *testing.T) {
			defer testhelper.ModifyEnvironment(t, "GITALY_TESTING_NO_GIT_HOOKS", "1")()
			require.Equal(t, "/var/empty", gitCmdFactory.HooksPath())
		})
	})

	t.Run("hooks path", func(t *testing.T) {
		gitCmdFactory := gittest.NewCommandFactory(t, config.Cfg{
			Git: config.Git{
				HooksPath: "/hooks/path",
			},
			Ruby: config.Ruby{
				Dir: rubyDir,
			},
		})

		defer testhelper.ModifyEnvironment(t, "GITALY_TESTING_NO_GIT_HOOKS", "1")()

		// The environment variable shouldn't override an explicitly set hooks path.
		require.Equal(t, "/hooks/path", gitCmdFactory.HooksPath())
	})
}

type hookFileMode int

const (
	hookFileExists hookFileMode = 1 << (4 - 1 - iota)
	hookFileExecutable
)

func setupTempHookDirs(t *testing.T, m map[string]hookFileMode) string {
	tempDir := testhelper.TempDir(t)

	for hookName, mode := range m {
		if mode&hookFileExists > 0 {
			path := filepath.Join(tempDir, hookName)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

			require.NoError(t, os.WriteFile(filepath.Join(tempDir, hookName), nil, 0o644))

			if mode&hookFileExecutable > 0 {
				require.NoError(t, os.Chmod(filepath.Join(tempDir, hookName), 0o755))
			}
		}
	}

	return tempDir
}

var (
	fileNotExistsErrRegexSnippit  = "no such file or directory"
	fileNotExecutableRegexSnippit = "not executable: .*"
)

func TestExecCommandFactory_ValidateHooks(t *testing.T) {
	testCases := []struct {
		desc             string
		expectedErrRegex string
		hookFiles        map[string]hookFileMode
	}{
		{
			desc: "everything is ✅",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       hookFileExists | hookFileExecutable,
				"ruby/git-hooks/pre-receive":  hookFileExists | hookFileExecutable,
				"ruby/git-hooks/post-receive": hookFileExists | hookFileExecutable,
			},
			expectedErrRegex: "",
		},
		{
			desc: "missing git-hooks",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       0,
				"ruby/git-hooks/pre-receive":  0,
				"ruby/git-hooks/post-receive": 0,
			},
			expectedErrRegex: fmt.Sprintf("%s, %s, %s", fileNotExistsErrRegexSnippit, fileNotExistsErrRegexSnippit, fileNotExistsErrRegexSnippit),
		},
		{
			desc: "git-hooks are not executable",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       hookFileExists,
				"ruby/git-hooks/pre-receive":  hookFileExists,
				"ruby/git-hooks/post-receive": hookFileExists,
			},
			expectedErrRegex: fmt.Sprintf("%s, %s, %s", fileNotExecutableRegexSnippit, fileNotExecutableRegexSnippit, fileNotExecutableRegexSnippit),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tempHookDir := setupTempHookDirs(t, tc.hookFiles)

			_, cleanup, err := git.NewExecCommandFactory(config.Cfg{
				Ruby: config.Ruby{
					Dir: filepath.Join(tempHookDir, "ruby"),
				},
				GitlabShell: config.GitlabShell{
					Dir: filepath.Join(tempHookDir, "/gitlab-shell"),
				},
				BinDir: filepath.Join(tempHookDir, "/bin"),
			})
			if err == nil {
				defer cleanup()
			}

			if tc.expectedErrRegex != "" {
				require.Error(t, err)
				require.Regexp(t, tc.expectedErrRegex, err.Error(), "error should match regexp")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
