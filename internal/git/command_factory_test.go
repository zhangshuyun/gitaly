package git

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestGitCommandProxy(t *testing.T) {
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

	dir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	cmd, err := NewCommandWithoutRepo(ctx, nil, SubCmd{
		Name: "clone",
		Args: []string{"http://gitlab.com/bogus-repo", dir},
	}, WithDisabledHooks())
	require.NoError(t, err)

	err = cmd.Wait()
	require.NoError(t, err)
	require.True(t, requestReceived)
}

func TestExecCommandFactory_NewWithDir(t *testing.T) {
	gitCmdFactory := NewExecCommandFactory(config.Config)

	t.Run("no dir specified", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		_, err := gitCmdFactory.NewWithDir(ctx, "", nil, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no 'dir' provided")
	})

	t.Run("runs in dir", func(t *testing.T) {
		_, repoPath, cleanup := testhelper.NewTestRepoWithWorktree(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		cmd, err := gitCmdFactory.NewWithDir(ctx, repoPath, nil, SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		}, WithStderr(&stderr))
		require.NoError(t, err)

		revData, err := ioutil.ReadAll(cmd)
		require.NoError(t, err)

		require.NoError(t, cmd.Wait(), stderr.String())

		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", text.ChompBytes(revData))
	})

	t.Run("doesn't runs in non existing dir", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		_, err := gitCmdFactory.NewWithDir(ctx, "non-existing-dir", nil, SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		}, WithStderr(&stderr))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})
}
