package maintenance

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestRandomWalk(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		dirs          []string
		files         []string
		skipPaths     []string
		expectedPaths []string
	}{
		{
			desc: "single directory",
			dirs: []string{
				"foo",
			},
			expectedPaths: []string{
				"foo",
			},
		},
		{
			desc: "multiple directories",
			dirs: []string{
				"foo/bar/baz",
				"foo/bar/qux",
				"foo/bar/qux/qax",
				"intermittent",
				"other/dir",
				"last",
			},
			expectedPaths: []string{
				"foo",
				"foo/bar",
				"foo/bar/qux",
				"foo/bar/qux/qax",
				"foo/bar/baz",
				"intermittent",
				"other",
				"other/dir",
				"last",
			},
		},
		{
			desc: "single file",
			files: []string{
				"file",
			},
			expectedPaths: []string{
				"file",
			},
		},
		{
			desc: "mixed files and directories",
			dirs: []string{
				"foo/bar/qux",
			},
			files: []string{
				"file1",
				"file2",
				"file3",
				"foo/file1",
				"foo/file2",
				"foo/file3",
				"foo/bar/qux/file1",
				"foo/bar/qux/file2",
				"foo/bar/qux/file3",
			},
			expectedPaths: []string{
				"file1",
				"file2",
				"foo",
				"foo/bar",
				"foo/bar/qux",
				"foo/bar/qux/file2",
				"foo/bar/qux/file3",
				"foo/bar/qux/file1",
				"foo/file2",
				"foo/file3",
				"foo/file1",
				"file3",
			},
		},
		{
			desc: "single skipped dir",
			dirs: []string{
				"foo",
			},
			skipPaths: []string{
				"foo",
			},
			expectedPaths: []string{
				"foo",
			},
		},
		{
			desc: "single skipped dir with nested contents",
			dirs: []string{
				"foo",
				"foo/subdir",
			},
			files: []string{
				"foo/file",
			},
			skipPaths: []string{
				"foo",
			},
			expectedPaths: []string{
				"foo",
			},
		},
		{
			desc: "mixed files and directories with skipping",
			dirs: []string{
				"dir1/subdir/subsubdir",
				"dir2/foo/bar/qux",
				"dir2/foo/baz",
				"dir3",
			},
			files: []string{
				"file",
				"dir2/foo/file",
			},
			skipPaths: []string{
				"dir1",
				"dir2/foo/bar",
			},
			expectedPaths: []string{
				"dir1",
				"dir2",
				"dir2/foo",
				"dir2/foo/file",
				"dir2/foo/bar",
				"dir2/foo/baz",
				"file",
				"dir3",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			root := testhelper.TempDir(t)

			for _, dir := range tc.dirs {
				require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0777))
			}

			for _, file := range tc.files {
				require.NoError(t, ioutil.WriteFile(filepath.Join(root, file), []byte{}, 0777))
			}

			walker := newRandomWalker(root, rand.New(rand.NewSource(1)))

			skipPaths := make(map[string]bool)
			for _, skipPath := range tc.skipPaths {
				skipPaths[filepath.Join(root, skipPath)] = true
			}

			actualPaths := []string{}
			for {
				fi, path, err := walker.next()
				if err == errIterOver {
					break
				}
				require.NoError(t, err)

				if skipPaths[path] {
					walker.skipDir()
				}

				require.Equal(t, filepath.Base(path), fi.Name())
				actualPaths = append(actualPaths, path)
			}

			expectedPaths := make([]string, len(tc.expectedPaths))
			for i, expectedPath := range tc.expectedPaths {
				expectedPaths[i] = filepath.Join(root, expectedPath)
			}

			require.Equal(t, expectedPaths, actualPaths)
		})
	}
}

func TestRandomWalk_withRemovedDirs(t *testing.T) {
	root := testhelper.TempDir(t)

	for _, dir := range []string{"foo/bar", "foo/bar/deleteme", "foo/baz/qux", "foo/baz/other"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0777))
	}

	walker := newRandomWalker(root, rand.New(rand.NewSource(1)))

	for _, expectedPath := range []string{"foo", "foo/bar"} {
		_, path, err := walker.next()
		require.NoError(t, err)
		require.Equal(t, filepath.Join(root, expectedPath), path)
	}

	require.NoError(t, os.RemoveAll(filepath.Join(root, "foo/bar")))

	_, path, err := walker.next()
	require.Error(t, err, "expected ENOENT")
	require.True(t, os.IsNotExist(err))
	require.Equal(t, filepath.Join(root, "foo/bar"), path)

	for _, expectedPath := range []string{"foo/baz", "foo/baz/other", "foo/baz/qux"} {
		_, path, err := walker.next()
		require.NoError(t, err)
		require.Equal(t, filepath.Join(root, expectedPath), path)
	}

	_, _, err = walker.next()
	require.Equal(t, err, errIterOver)
}
