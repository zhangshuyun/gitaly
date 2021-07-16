package tempdir

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

const (
	// GitalyDataPrefix is the top-level directory we use to store system
	// (non-user) data. We need to be careful that this path does not clash
	// with any directory name that could be provided by a user. The '+'
	// character is not allowed in GitLab namespaces or repositories.
	GitalyDataPrefix = config.GitalyDataPrefix

	// tmpRootPrefix is the directory in which we store temporary
	// directories.
	tmpRootPrefix = GitalyDataPrefix + "/tmp"

	// cachePrefix is the directory where all cache data is stored on a
	// storage location.
	cachePrefix = GitalyDataPrefix + "/cache"

	// statePrefix is the directory where all state data is stored on a
	// storage location.
	statePrefix = GitalyDataPrefix + "/state"

	// MaxAge is used by ForDeleteAllRepositories. It is also a fallback
	// for the context-scoped temporary directories, to ensure they get
	// cleaned up if the cleanup at the end of the context failed to run.
	MaxAge = 7 * 24 * time.Hour
)

// CacheDir returns the path to the cache dir for a storage location
func CacheDir(storage config.Storage) string { return AppendCacheDir(storage.Path) }

// AppendCacheDir will append the cache directory convention to the storage path
// provided
func AppendCacheDir(storagePath string) string { return filepath.Join(storagePath, cachePrefix) }

// AppendStateDir will append the state directory convention to the storage path
// provided
func AppendStateDir(storagePath string) string { return filepath.Join(storagePath, statePrefix) }

// TempDir returns the path to the temp dir for a storage location
func TempDir(storage config.Storage) string { return AppendTempDir(storage.Path) }

// AppendTempDir will append the temp directory convention to the storage path
// provided
func AppendTempDir(storagePath string) string { return filepath.Join(storagePath, tmpRootPrefix) }

// StartCleaning starts tempdir cleanup in a goroutine.
func StartCleaning(storages []config.Storage, d time.Duration) {
	dontpanic.Go(func() {
		for {
			cleanTempDir(storages)
			time.Sleep(d)
		}
	})
}

func cleanTempDir(storages []config.Storage) {
	for _, storage := range storages {
		start := time.Now()
		err := clean(TempDir(storage))

		entry := logrus.WithFields(logrus.Fields{
			"time_ms": time.Since(start).Milliseconds(),
			"storage": storage.Name,
		})
		if err != nil {
			entry = entry.WithError(err)
		}
		entry.Info("finished tempdir cleaner walk")
	}
}

type invalidCleanRoot string

func clean(dir string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If we start "cleaning up" the wrong directory we may delete user data
	// which is Really Bad.
	if !strings.HasSuffix(dir, tmpRootPrefix) {
		logrus.Print(dir)
		panic(invalidCleanRoot("invalid tempdir clean root: panicking to prevent data loss"))
	}

	entries, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	for _, info := range entries {
		if time.Since(info.ModTime()) < MaxAge {
			continue
		}

		fullPath := filepath.Join(dir, info.Name())
		if err := housekeeping.FixDirectoryPermissions(ctx, fullPath); err != nil {
			return err
		}

		if err := os.RemoveAll(fullPath); err != nil {
			return err
		}
	}

	return nil
}
