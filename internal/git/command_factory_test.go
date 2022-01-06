package git_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
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
