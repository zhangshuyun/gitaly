package repository

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateRepositoryFromURLRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	importedRepo := &gitalypb.Repository{
		RelativePath: "imports/test-repo-imported.git",
		StorageName:  testhelper.DefaultStorageName,
	}

	_, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	user := "username123"
	password := "password321localhost"
	port, stopGitServer := gitServerWithBasicAuth(t, user, password, testRepoPath)
	defer stopGitServer()

	url := fmt.Sprintf("http://%s:%s@localhost:%d/%s", user, password, port, filepath.Base(testRepoPath))

	req := &gitalypb.CreateRepositoryFromURLRequest{
		Repository: importedRepo,
		Url:        url,
	}

	_, err := client.CreateRepositoryFromURL(ctx, req)
	require.NoError(t, err)

	importedRepoPath, err := helper.GetRepoPath(importedRepo)
	require.NoError(t, err)
	defer os.RemoveAll(importedRepoPath)

	testhelper.MustRunCommand(t, nil, "git", "-C", importedRepoPath, "fsck")

	remotes := testhelper.MustRunCommand(t, nil, "git", "-C", importedRepoPath, "remote")
	require.NotContains(t, string(remotes), "origin")

	info, err := os.Lstat(path.Join(importedRepoPath, "hooks"))
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)
}

func TestCloneRepositoryFromUrlCommand(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	userInfo := "user:pass%21%3F%40"
	repositoryFullPath := "full/path/to/repository"
	url := fmt.Sprintf("https://%s@www.example.com/secretrepo.git", userInfo)

	cmd, err := cloneFromURLCommand(ctx, url, repositoryFullPath)
	require.NoError(t, err)

	expectedScrubbedURL := "https://www.example.com/secretrepo.git"
	expectedBasicAuthHeader := fmt.Sprintf("Authorization: Basic %s", base64.StdEncoding.EncodeToString([]byte("user:pass!?@")))
	expectedHeader := fmt.Sprintf("http.%s.extraHeader=%s", expectedScrubbedURL, expectedBasicAuthHeader)

	var args = cmd.Args()
	require.Contains(t, args, expectedScrubbedURL)
	require.Contains(t, args, expectedHeader)
	require.NotContains(t, args, userInfo)
}

func TestFailedCreateRepositoryFromURLRequestDueToExistingTarget(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc     string
		repoPath string
		isDir    bool
	}{
		{
			desc:     "target is a directory",
			repoPath: "imports/test-repo-import-dir.git",
			isDir:    true,
		},
		{
			desc:     "target is a file",
			repoPath: "imports/test-repo-import-file.git",
			isDir:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			importedRepo := &gitalypb.Repository{
				RelativePath: "imports/test-repo-imported.git",
				StorageName:  testhelper.DefaultStorageName,
			}

			importedRepoPath, err := helper.GetPath(importedRepo)
			require.NoError(t, err)

			if testCase.isDir {
				require.NoError(t, os.MkdirAll(importedRepoPath, 0770))
			} else {
				require.NoError(t, ioutil.WriteFile(importedRepoPath, nil, 0644))
			}
			defer os.RemoveAll(importedRepoPath)

			req := &gitalypb.CreateRepositoryFromURLRequest{
				Repository: importedRepo,
				Url:        "https://gitlab.com/gitlab-org/gitlab-test.git",
			}

			_, err = client.CreateRepositoryFromURL(ctx, req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestPreventingRedirect(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	importedRepo := &gitalypb.Repository{
		RelativePath: "imports/test-repo-imported.git",
		StorageName:  testhelper.DefaultStorageName,
	}

	httpServerState, redirectingServer := StartRedirectingTestServer()
	defer redirectingServer.Close()

	req := &gitalypb.CreateRepositoryFromURLRequest{
		Repository: importedRepo,
		Url:        redirectingServer.URL,
	}

	_, err := client.CreateRepositoryFromURL(ctx, req)

	require.True(t, httpServerState.serverVisited, "git command should make the initial HTTP request")
	require.False(t, httpServerState.serverVisitedAfterRedirect, "git command should not follow HTTP redirection")

	require.Error(t, err)
}

func gitServerWithBasicAuth(t testing.TB, user, pass, repoPath string) (int, func() error) {
	return testhelper.GitServer(t, repoPath, basicAuthMiddleware(t, user, pass))
}

func basicAuthMiddleware(t testing.TB, user, pass string) func(http.ResponseWriter, *http.Request, http.Handler) {
	return func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		authUser, authPass, ok := r.BasicAuth()
		require.True(t, ok, "should contain basic auth")
		require.Equal(t, user, authUser, "username should match")
		require.Equal(t, pass, authPass, "password should match")
		next.ServeHTTP(w, r)
	}
}
