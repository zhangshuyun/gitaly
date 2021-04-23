package safe_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/safe"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestFile(t *testing.T) {
	dir := testhelper.TempDir(t)

	filePath := filepath.Join(dir, "test_file_contents")
	fileContents := "very important contents"
	file, err := safe.CreateFileWriter(filePath)
	require.NoError(t, err)

	_, err = io.Copy(file, bytes.NewBufferString(fileContents))
	require.NoError(t, err)

	testhelper.AssertPathNotExists(t, filePath)

	require.NoError(t, file.Commit())

	writtenContents, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, fileContents, string(writtenContents))

	filesInTempDir, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, filesInTempDir, 1)
	require.Equal(t, filepath.Base(filePath), filesInTempDir[0].Name())
}

func TestFileRace(t *testing.T) {
	dir := testhelper.TempDir(t)

	filePath := filepath.Join(dir, "test_file_contents")

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			w, err := safe.CreateFileWriter(filePath)
			require.NoError(t, err)
			_, err = w.Write([]byte(fmt.Sprintf("message # %d", i)))
			require.NoError(t, err)
			require.NoError(t, w.Commit())
			wg.Done()
		}(i)
	}
	wg.Wait()

	require.FileExists(t, filePath)
	filesInTempDir, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, filesInTempDir, 1, "make sure no other files were written")
}

func TestFileCloseBeforeCommit(t *testing.T) {
	dir := testhelper.TempDir(t)

	dstPath := filepath.Join(dir, "safety_meow")
	sf, err := safe.CreateFileWriter(dstPath)
	require.NoError(t, err)

	require.True(t, !dirEmpty(t, dir), "should contain something")

	_, err = sf.Write([]byte("MEOW MEOW MEOW MEOW"))
	require.NoError(t, err)

	require.NoError(t, sf.Close())
	require.True(t, dirEmpty(t, dir), "should be empty")

	require.Equal(t, safe.ErrAlreadyDone, sf.Commit())
}

func TestFileCommitBeforeClose(t *testing.T) {
	dir := testhelper.TempDir(t)

	dstPath := filepath.Join(dir, "safety_meow")
	sf, err := safe.CreateFileWriter(dstPath)
	require.NoError(t, err)

	require.False(t, dirEmpty(t, dir), "should contain something")

	_, err = sf.Write([]byte("MEOW MEOW MEOW MEOW"))
	require.NoError(t, err)

	require.NoError(t, sf.Commit())
	require.FileExists(t, dstPath)

	require.Equal(t, safe.ErrAlreadyDone, sf.Close(),
		"Close should be impotent after call to commit",
	)
	require.FileExists(t, dstPath)
}

func dirEmpty(t testing.TB, dirPath string) bool {
	infos, err := ioutil.ReadDir(dirPath)
	require.NoError(t, err)
	return len(infos) == 0
}
