package storage

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestListDirectories(t *testing.T) {
	testDir := path.Join(testStorage.Path, t.Name())
	require.NoError(t, os.MkdirAll(path.Dir(testDir), 0755))
	defer os.RemoveAll(testDir)

	// Mock the storage dir being our test dir, so results aren't influenced
	// by other tests.
	testStorages := []config.Storage{{Name: "default", Path: testDir}}

	defer func(oldStorages []config.Storage) {
		config.ModifyStorages(oldStorages)
	}(config.Config.Storages)
	config.ModifyStorages(testStorages)

	repoPaths := []string{"foo", "bar", "bar/baz", "bar/baz/foo/buz"}
	for _, p := range repoPaths {
		dirPath := filepath.Join(testDir, p)
		require.NoError(t, os.MkdirAll(dirPath, 0755))
		require.NoError(t, ioutil.WriteFile(filepath.Join(dirPath, "file"), []byte("Hello"), 0644))
	}

	server, socketPath := runStorageServer(t)
	defer server.Stop()

	client, conn := newStorageClient(t, socketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		depth uint32
		dirs  []string
	}{
		{
			depth: 0,
			dirs:  []string{"bar", "foo"},
		},
		{
			depth: 1,
			dirs:  []string{"bar", "bar/baz", "foo"},
		},
		{
			depth: 3,
			dirs:  []string{"bar", "bar/baz", "bar/baz/foo", "bar/baz/foo/buz", "foo"},
		},
	}

	for _, tc := range testCases {
		stream, err := client.ListDirectories(ctx, &gitalypb.ListDirectoriesRequest{StorageName: "default", Depth: tc.depth})

		var dirs []string
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}

			require.NoError(t, err)
			dirs = append(dirs, resp.GetPaths()...)
		}

		require.NoError(t, err)
		require.NotEmpty(t, dirs)
		assert.Equal(t, tc.dirs, dirs)

		for _, dir := range dirs {
			assert.False(t, strings.HasSuffix(dir, "file"))
		}
	}
}
