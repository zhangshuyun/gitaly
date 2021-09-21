package safe_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestLockingFileWriter_lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("normal lifecycle", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.NoError(t, writer.Lock())
		require.NoError(t, writer.Commit())
		require.NoError(t, writer.Close())
	})

	t.Run("multiple locks fail", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.NoError(t, writer.Lock())
		require.Equal(t, fmt.Errorf("file writer not lockable"), writer.Lock())
	})

	t.Run("commit without lock fails", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.Equal(t, fmt.Errorf("file writer not locked"), writer.Commit())
	})

	t.Run("multiple commits fail", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.NoError(t, writer.Lock())
		require.NoError(t, writer.Commit())
		require.Equal(t, fmt.Errorf("file writer not locked"), writer.Commit())
	})

	t.Run("lock after close fails", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.Equal(t, fmt.Errorf("file writer not lockable"), writer.Lock())
	})

	t.Run("multiple closes succeed", func(t *testing.T) {
		writer, err := safe.NewLockingFileWriter(filepath.Join(testhelper.TempDir(t), "file"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, writer.Close())
	})
}

func TestLockingFileWriter_stateCleanup(t *testing.T) {
	t.Parallel()

	t.Run("commit", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")
		lock := file + ".lock"

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		require.FileExists(t, writer.Path())
		require.NoFileExists(t, lock)

		require.NoError(t, writer.Lock())
		require.FileExists(t, writer.Path())
		require.FileExists(t, lock)

		require.NoError(t, writer.Commit())
		require.NoFileExists(t, writer.Path())
		require.NoFileExists(t, lock)
	})

	t.Run("close", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")
		lock := file + ".lock"

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		require.FileExists(t, writer.Path())
		require.NoFileExists(t, lock)

		require.NoError(t, writer.Lock())
		require.FileExists(t, writer.Path())
		require.FileExists(t, lock)

		require.NoError(t, writer.Close())
		require.NoFileExists(t, writer.Path())
		require.NoFileExists(t, lock)
	})
}

func TestLockingFileWriter_createsNewFiles(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)
	_, err = writer.Write([]byte("created"))
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte("created"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_createsEmptyFiles(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte{}, testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_seedingWithNonExistentTarget(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	writer, err := safe.NewLockingFileWriter(target, safe.LockingFileWriterConfig{
		SeedContents: true,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte{}, testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_seedingWithExistingTarget(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")
	require.NoError(t, os.WriteFile(target, []byte("seed"), 0o644))

	writer, err := safe.NewLockingFileWriter(target, safe.LockingFileWriterConfig{
		SeedContents: true,
	})
	require.NoError(t, err)
	_, err = writer.Write([]byte("append"))
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte("seedappend"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_modifiesExistingFiles(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")
	require.NoError(t, os.WriteFile(target, []byte("preexisting"), 0o644))

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)
	_, err = writer.Write([]byte("modified"))
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte("modified"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_modifiesExistingFilesWithMode(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")
	require.NoError(t, os.WriteFile(target, []byte("preexisting"), 0o644))

	writer, err := safe.NewLockingFileWriter(target, safe.LockingFileWriterConfig{
		FileWriterConfig: safe.FileWriterConfig{FileMode: 0o060},
	})
	require.NoError(t, err)
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	fi, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o060), fi.Mode())
}

func TestLockingFileWriter_concurrentCreation(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)

	// Create file concurrently.
	require.NoError(t, os.WriteFile(target, []byte("concurrent"), 0o644))

	require.Equal(t, fmt.Errorf("file concurrently created"), writer.Lock())

	require.Equal(t, []byte("concurrent"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_concurrentDeletion(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	require.NoError(t, os.WriteFile(target, []byte("base"), 0o644))
	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)

	// Delete file concurrently.
	require.NoError(t, os.Remove(target))

	require.Equal(t, fmt.Errorf("file concurrently deleted"), writer.Lock())

	require.NoFileExists(t, target)
}

func TestLockingFileWriter_concurrentModification(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")

	require.NoError(t, os.WriteFile(target, []byte("base"), 0o644))
	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)

	// Concurrently modify the file.
	require.NoError(t, os.WriteFile(target, []byte("concurrent"), 0o644))

	require.Equal(t, fmt.Errorf("file concurrently modified"), writer.Lock())

	require.Equal(t, []byte("concurrent"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_concurrentLocking(t *testing.T) {
	t.Parallel()

	file := filepath.Join(testhelper.TempDir(t), "file")

	first, err := safe.NewLockingFileWriter(file)
	require.NoError(t, err)
	_, err = first.Write([]byte("first"))
	require.NoError(t, err)

	second, err := safe.NewLockingFileWriter(file)
	require.NoError(t, err)
	_, err = second.Write([]byte("second"))
	require.NoError(t, err)

	require.NoError(t, first.Lock())
	require.Equal(t, fmt.Errorf("file already locked"), second.Lock())
	require.NoError(t, first.Commit())

	require.Equal(t, []byte("first"), testhelper.MustReadFile(t, file))
}

func TestLockingFileWriter_locked(t *testing.T) {
	t.Parallel()

	target := filepath.Join(testhelper.TempDir(t), "file")
	require.NoError(t, os.WriteFile(target, []byte("base"), 0o644))

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)

	// Concurrently lock the file.
	require.NoError(t, os.WriteFile(target+".lock", nil, 0o644))

	require.Equal(t, fmt.Errorf("file already locked"), writer.Lock())

	require.Equal(t, []byte("base"), testhelper.MustReadFile(t, target))
}

func TestLockingFileWriter_externalProcess(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	target := filepath.Join(testhelper.TempDir(t), "file")
	require.NoError(t, os.WriteFile(target, []byte("base"), 0o644))

	writer, err := safe.NewLockingFileWriter(target)
	require.NoError(t, err)

	gittest.Exec(t, cfg, "config", "-f", writer.Path(), "some.config", "true")
	require.NoError(t, writer.Lock())
	require.NoError(t, writer.Commit())

	require.Equal(t, []byte("[some]\n\tconfig = true\n"), testhelper.MustReadFile(t, target))
}
