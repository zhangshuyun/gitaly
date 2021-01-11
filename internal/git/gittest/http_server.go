package gittest

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
)

// RemoteUploadPackServer implements two HTTP routes for git-upload-pack by copying stdin and stdout into and out of the git upload-pack command
func RemoteUploadPackServer(ctx context.Context, t *testing.T, gitPath, repoName, httpToken, repoPath string) (*httptest.Server, string) {
	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.String() {
			case fmt.Sprintf("/%s.git/git-upload-pack", repoName):
				w.WriteHeader(http.StatusOK)

				var err error
				reader := r.Body

				if r.Header.Get("Content-Encoding") == "gzip" {
					reader, err = gzip.NewReader(r.Body)
					require.NoError(t, err)
				}
				defer r.Body.Close()

				cmd, err := command.New(ctx, exec.Command(gitPath, "-C", repoPath, "upload-pack", "--stateless-rpc", "."), reader, w, nil)
				require.NoError(t, err)
				require.NoError(t, cmd.Wait())
			case fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName):
				if httpToken != "" && r.Header.Get("Authorization") != httpToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
				w.WriteHeader(http.StatusOK)

				w.Write([]byte("001e# service=git-upload-pack\n"))
				w.Write([]byte("0000"))

				cmd, err := command.New(ctx, exec.Command(gitPath, "-C", repoPath, "upload-pack", "--advertise-refs", "."), nil, w, nil)
				require.NoError(t, err)
				require.NoError(t, cmd.Wait())
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}
