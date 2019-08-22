package repository

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateRepositoryFromURLRequest(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	importedRepo := &gitalypb.Repository{
		RelativePath: "imports/test-repo-imported.git",
		StorageName:  testhelper.DefaultStorageName,
	}

	req := &gitalypb.CreateRepositoryFromURLRequest{
		Repository: importedRepo,
		Url:        "https://gitlab.com/gitlab-org/gitlab-test.git",
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

func TestFailedCreateRepositoryFromURLRequestDueToExistingTarget(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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
