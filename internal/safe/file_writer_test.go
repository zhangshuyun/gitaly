package safe_test

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestFileWriter_successful(t *testing.T) {
	dir := testhelper.TempDir(t)

	filePath := filepath.Join(dir, "test_file_contents")
	fileContents := "very important contents"
	file, err := safe.NewFileWriter(filePath)
	require.NoError(t, err)

	_, err = io.Copy(file, bytes.NewBufferString(fileContents))
	require.NoError(t, err)

	require.NoFileExists(t, filePath)

	require.NoError(t, file.Commit())

	writtenContents := testhelper.MustReadFile(t, filePath)
	require.Equal(t, fileContents, string(writtenContents))

	filesInTempDir, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, filesInTempDir, 1)
	require.Equal(t, filepath.Base(filePath), filesInTempDir[0].Name())
}

func TestFileWriter_multipleConfigs(t *testing.T) {
	_, err := safe.NewFileWriter("something", safe.FileWriterConfig{},
		safe.FileWriterConfig{})
	require.Equal(t, fmt.Errorf("file writer created with more than one config"), err)
}

func TestFileWriter_mode(t *testing.T) {
	dir := testhelper.TempDir(t)

	target := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(target, []byte("contents"), 0o600))

	writer, err := safe.NewFileWriter(target, safe.FileWriterConfig{
		FileMode: 0o060,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Commit())

	fi, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o060), fi.Mode())
}

func TestFileWriter_race(t *testing.T) {
	dir := testhelper.TempDir(t)

	filePath := filepath.Join(dir, "test_file_contents")

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			w, err := safe.NewFileWriter(filePath)
			require.NoError(t, err)
			_, err = w.Write([]byte(fmt.Sprintf("message # %d", i)))
			require.NoError(t, err)
			require.NoError(t, w.Commit())
			wg.Done()
		}(i)
	}
	wg.Wait()

	require.FileExists(t, filePath)
	filesInTempDir, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, filesInTempDir, 1, "make sure no other files were written")
}

func TestFileWriter_closeBeforeCommit(t *testing.T) {
	dir := testhelper.TempDir(t)

	dstPath := filepath.Join(dir, "safety_meow")
	sf, err := safe.NewFileWriter(dstPath)
	require.NoError(t, err)

	require.True(t, !dirEmpty(t, dir), "should contain something")

	_, err = sf.Write([]byte("MEOW MEOW MEOW MEOW"))
	require.NoError(t, err)

	require.NoError(t, sf.Close())
	require.True(t, dirEmpty(t, dir), "should be empty")

	require.Equal(t, safe.ErrAlreadyDone, sf.Commit())
}

func TestFileWriter_commitBeforeClose(t *testing.T) {
	dir := testhelper.TempDir(t)

	dstPath := filepath.Join(dir, "safety_meow")
	sf, err := safe.NewFileWriter(dstPath)
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
	infos, err := os.ReadDir(dirPath)
	require.NoError(t, err)
	return len(infos) == 0
}
