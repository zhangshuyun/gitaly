package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestDeleteAllSuccess(t *testing.T) {
	require.NoError(t, os.RemoveAll(testStorage.Path))

	gitalyDataFile := path.Join(testStorage.Path, tempdir.GitalyDataPrefix+"/foobar")
	require.NoError(t, os.MkdirAll(path.Dir(gitalyDataFile), 0755))
	require.NoError(t, ioutil.WriteFile(gitalyDataFile, nil, 0644))

	repoPaths := []string{
		"foo/bar1.git",
		"foo/bar2.git",
		"baz/foo/qux3.git",
		"baz/foo/bar1.git",
	}

	for _, p := range repoPaths {
		fullPath := path.Join(testStorage.Path, p)
		require.NoError(t, os.MkdirAll(fullPath, 0755))
		require.NoError(t, exec.Command("git", "init", "--bare", fullPath).Run())
	}

	dirents := storageDirents(t, testStorage)
	expectedNames := []string{"+gitaly", "baz", "foo"}
	require.Len(t, dirents, len(expectedNames))
	for i, expected := range expectedNames {
		require.Equal(t, expected, dirents[i].Name())
	}

	server, socketPath := runStorageServer(t)
	defer server.Stop()

	client, conn := newStorageClient(t, socketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()
	_, err := client.DeleteAllRepositories(ctx, &gitalypb.DeleteAllRepositoriesRequest{StorageName: testStorage.Name})
	require.NoError(t, err)

	dirents = storageDirents(t, testStorage)
	require.Len(t, dirents, 1)
	require.Equal(t, "+gitaly", dirents[0].Name())

	_, err = os.Stat(gitalyDataFile)
	require.NoError(t, err, "unrelated data file should still exist")
}

func storageDirents(t *testing.T, st config.Storage) []os.FileInfo {
	dirents, err := ioutil.ReadDir(st.Path)
	require.NoError(t, err)
	return dirents
}

func TestDeleteAllFail(t *testing.T) {
	server, socketPath := runStorageServer(t)
	defer server.Stop()

	client, conn := newStorageClient(t, socketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc  string
		req   *gitalypb.DeleteAllRepositoriesRequest
		setup func(t *testing.T)
		code  codes.Code
	}{
		{
			desc: "empty storage name",
			req:  &gitalypb.DeleteAllRepositoriesRequest{},
			code: codes.InvalidArgument,
		},
		{
			desc: "unknown storage name",
			req:  &gitalypb.DeleteAllRepositoriesRequest{StorageName: "does not exist"},
			code: codes.InvalidArgument,
		},
		{
			desc: "cannot create trash dir",
			req:  &gitalypb.DeleteAllRepositoriesRequest{StorageName: testStorage.Name},
			setup: func(t *testing.T) {
				dataDir := path.Join(testStorage.Path, tempdir.GitalyDataPrefix)
				require.NoError(t, os.RemoveAll(dataDir))
				require.NoError(t, ioutil.WriteFile(dataDir, nil, 0644), "write file where there should be a directory")

				lsOut, err := exec.Command("ls", "-l", testStorage.Path).CombinedOutput()
				require.NoError(t, err)
				fmt.Printf("%s\n", lsOut)
			},
			code: codes.Internal,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, os.RemoveAll(testStorage.Path))
			require.NoError(t, os.MkdirAll(testStorage.Path, 0755))

			repoPath := path.Join(testStorage.Path, "foobar.git")
			require.NoError(t, exec.Command("git", "init", "--bare", repoPath).Run())

			if tc.setup != nil {
				tc.setup(t)
			}

			_, err := client.DeleteAllRepositories(ctx, tc.req)
			require.Equal(t, tc.code, status.Code(err), "expected grpc status code")

			_, err = os.Stat(repoPath)
			require.NoError(t, err, "repo must still exist")
		})
	}
}
