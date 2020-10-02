package git

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestGitCommandProxy(t *testing.T) {
	requestReceived := false

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
	}))
	defer ts.Close()

	oldHTTPProxy := os.Getenv("http_proxy")
	defer os.Setenv("http_proxy", oldHTTPProxy)

	os.Setenv("http_proxy", ts.URL)

	ctx, cancel := testhelper.Context()
	defer cancel()

	dir, err := ioutil.TempDir("", "test-clone")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cmd, err := unsafeCmdWithoutRepo(ctx, CmdStream{}, "clone", "http://gitlab.com/bogus-repo", dir)
	require.NoError(t, err)

	err = cmd.Wait()
	require.NoError(t, err)
	require.True(t, requestReceived)
}
