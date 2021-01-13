package git

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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

	cmd, err := NewCommandFactory().unsafeBareCmd(ctx, cmdStream{}, nil, "clone", "http://gitlab.com/bogus-repo", dir)
	require.NoError(t, err)

	err = cmd.Wait()
	require.NoError(t, err)
	require.True(t, requestReceived)
}
