package backup

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestFilesystemSink_GetReader(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir := testhelper.TempDir(t)
		const relativePath = "test.dat"
		require.NoError(t, os.WriteFile(filepath.Join(dir, relativePath), []byte("test"), 0o644))

		fsSink := NewFilesystemSink(dir)
		reader, err := fsSink.GetReader(ctx, relativePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, reader.Close()) }()

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("no file", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir, err := os.Getwd()
		require.NoError(t, err)

		fsSink := NewFilesystemSink(dir)
		reader, err := fsSink.GetReader(ctx, "not-existing")
		require.Equal(t, fmt.Errorf(`filesystem sink: get reader for "not-existing": %w`, ErrDoesntExist), err)
		require.Nil(t, reader)
	})
}

func TestFilesystemSink_Write(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir := testhelper.TempDir(t)
		const relativePath = "nested/dir/test.dat"

		fsSink := NewFilesystemSink(dir)
		require.NoError(t, fsSink.Write(ctx, relativePath, strings.NewReader("test")))

		require.FileExists(t, filepath.Join(dir, relativePath))
		data, err := os.ReadFile(filepath.Join(dir, relativePath))
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("no data doesn't create a file", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir := testhelper.TempDir(t)
		const relativePath = "nested/dir/test.dat"

		fsSink := NewFilesystemSink(dir)
		require.NoError(t, fsSink.Write(ctx, relativePath, strings.NewReader("")))

		require.NoFileExists(t, filepath.Join(dir, relativePath))
	})

	t.Run("overrides existing data", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir := testhelper.TempDir(t)
		const relativePath = "nested/dir/test.dat"
		fullPath := filepath.Join(dir, relativePath)

		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
		require.NoError(t, os.WriteFile(fullPath, []byte("initial"), 0o655))

		fsSink := NewFilesystemSink(dir)
		require.NoError(t, fsSink.Write(ctx, relativePath, strings.NewReader("test")))

		require.FileExists(t, fullPath)
		data, err := os.ReadFile(fullPath)
		require.NoError(t, err)
		require.Equal(t, []byte("test"), data)
	})

	t.Run("dir creation error", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		dir := testhelper.TempDir(t)
		const relativePath = "nested/test.dat"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "nested"), []byte("lock"), os.ModePerm))

		fsSink := NewFilesystemSink(dir)
		err := fsSink.Write(ctx, relativePath, strings.NewReader("test"))
		require.EqualError(t, err, fmt.Sprintf(`create directory structure %[1]q: mkdir %[1]s: not a directory`, filepath.Join(dir, "nested")))
	})
}
