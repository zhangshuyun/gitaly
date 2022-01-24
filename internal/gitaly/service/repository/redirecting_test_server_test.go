package repository

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

const redirectURL = "/redirect_url"

// RedirectingTestServerState holds information about whether the server was visited and redirect was happened
type RedirectingTestServerState struct {
	serverVisited              bool
	serverVisitedAfterRedirect bool
}

// StartRedirectingTestServer starts the test server with initial state
func StartRedirectingTestServer() (*RedirectingTestServerState, *httptest.Server) {
	state := &RedirectingTestServerState{}
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == redirectURL {
				state.serverVisitedAfterRedirect = true
			} else {
				state.serverVisited = true
				http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
			}
		}),
	)

	return state, server
}

func TestRedirectingServerRedirects(t *testing.T) {
	t.Parallel()
	cfg := testcfg.Build(t)
	dir := testhelper.TempDir(t)

	httpServerState, redirectingServer := StartRedirectingTestServer()
	ctx := testhelper.Context(t)

	var stderr bytes.Buffer
	cmd, err := gittest.NewCommandFactory(t, cfg).NewWithoutRepo(ctx, git.SubCmd{
		Name: "clone",
		Flags: []git.Option{
			git.Flag{Name: "--bare"},
		},
		Args: []string{
			redirectingServer.URL, dir,
		},
	}, git.WithConfig(git.ConfigPair{Key: "http.followRedirects", Value: "true"}), git.WithDisabledHooks(), git.WithStderr(&stderr))
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	require.Contains(t, stderr.String(), "unable to update url base from redirection")

	redirectingServer.Close()

	require.True(t, httpServerState.serverVisited, "git command should make the initial HTTP request")
	require.True(t, httpServerState.serverVisitedAfterRedirect, "git command should follow the HTTP redirect")
}
