package testhelper

import (
	"io/ioutil"
	"net"
	"net/http"
	"net/http/cgi"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// GitServer establishes a cgi git server.
func GitServer(t testing.TB, cfg config.Cfg, repoPath string, middleware func(http.ResponseWriter, *http.Request, http.Handler)) (int, func() error) {
	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, "git-daemon-export-ok"), nil, 0644))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	gitHTTPBackend := &cgi.Handler{
		Path: cfg.Git.BinPath,
		Dir:  "/",
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + filepath.Dir(repoPath),
		},
	}
	s := http.Server{Handler: gitHTTPBackend}

	if middleware != nil {
		s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middleware(w, r, gitHTTPBackend)
		})
	}

	go s.Serve(listener)

	return listener.Addr().(*net.TCPAddr).Port, s.Close
}
