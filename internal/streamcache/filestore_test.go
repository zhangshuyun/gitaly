package streamcache

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestFilestoreCreate(t *testing.T) {
	tmp := testhelper.TempDir(t)

	fs := newFilestore(tmp, 0, time.Sleep, log.Default())
	defer fs.Stop()

	f, err := fs.Create()
	require.NoError(t, err)

	_, err = f.Write([]byte{0})
	require.NoError(t, err, "file is writeable")
	require.NoError(t, f.Close())

	relpath, err := filepath.Rel(fs.dir, f.Name())
	require.NoError(t, err)
	require.Regexp(
		t,
		regexp.MustCompile(`^[0-9a-f]{2}/[^/]+$`),
		relpath,
		"path should be one directory deep and use aa/foobar hex scheme",
	)
}

func TestFilestoreCreate_concurrency(t *testing.T) {
	tmp := testhelper.TempDir(t)

	fs := newFilestore(tmp, time.Hour, time.Sleep, log.Default())
	defer fs.Stop()

	const N = 100

	errors := make(chan error, N)
	start := make(chan struct{})

	for i := 0; i < N; i++ {
		go func(i int) {
			<-start
			errors <- func() error {
				f, err := fs.Create()
				if err != nil {
					return err
				}
				if err := f.Close(); err != nil {
					return err
				}
				return os.Remove(f.Name())
			}()
		}(i)
	}

	close(start)

	for i := 0; i < N; i++ {
		require.NoError(t, <-errors)
	}
}

func TestFilestoreCreate_uniqueness(t *testing.T) {
	tmp := testhelper.TempDir(t)

	const (
		M = 10
		N = 10
	)

	filenames := make(map[string]struct{})

	for j := 0; j < M; j++ {
		fs := newFilestore(tmp, time.Hour, time.Sleep, log.Default())
		defer fs.Stop()

		for i := 0; i < N; i++ {
			t.Run(fmt.Sprintf("create file %d/%d", i, j), func(t *testing.T) {
				f, err := fs.Create()
				require.NoError(t, err)
				require.NoError(t, f.Close())

				filenames[f.Name()] = struct{}{}
			})
		}
	}

	require.Len(t, filenames, M*N, "all filenames must be unique")
}

func TestFilestoreCleanwalk(t *testing.T) {
	tmp := testhelper.TempDir(t)

	fs := newFilestore(tmp, time.Hour, time.Sleep, log.Default())
	defer fs.Stop()

	dir1 := filepath.Join(tmp, "dir1")
	dir2 := filepath.Join(tmp, "dir2")
	file := filepath.Join(dir2, "file")
	require.NoError(t, os.Mkdir(dir1, 0755))
	require.NoError(t, os.Mkdir(dir2, 0755))
	require.NoError(t, ioutil.WriteFile(file, nil, 0644))
	require.NoError(t, os.Chmod(dir2, 0), "create dir with pathological permissions")

	require.NoError(t, fs.cleanWalk(time.Now().Add(time.Hour)))

	for _, d := range []string{dir1, dir2} {
		fi, err := os.Stat(d)
		require.NoError(t, err, "directories do not get deleted")

		const mask = 0700
		require.True(t, fi.Mode()&mask >= mask, "unexpected file mode %o", fi.Mode())
	}

	require.NoFileExists(t, file)
}
