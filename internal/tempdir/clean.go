package tempdir

//nolint:depguard
import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
)

const (
	// tmpRootPrefix is the directory in which we store temporary directories.
	tmpRootPrefix = config.GitalyDataPrefix + "/tmp"

	// maxAge is used by ForDeleteAllRepositories. It is also a fallback for the context-scoped
	// temporary directories, to ensure they get cleaned up if the cleanup at the end of the
	// context failed to run.
	maxAge = 7 * 24 * time.Hour
)

// StartCleaning starts tempdir cleanup in a goroutine.
func StartCleaning(locator storage.Locator, storages []config.Storage, d time.Duration) {
	dontpanic.Go(func() {
		for {
			cleanTempDir(locator, storages)
			time.Sleep(d)
		}
	})
}

func cleanTempDir(locator storage.Locator, storages []config.Storage) {
	for _, storage := range storages {
		start := time.Now()
		err := clean(locator, storage)

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

func clean(locator storage.Locator, storage config.Storage) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir, err := locator.TempDir(storage.Name)
	if err != nil {
		return fmt.Errorf("temporary dir: %w", err)
	}

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
		if time.Since(info.ModTime()) < maxAge {
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
