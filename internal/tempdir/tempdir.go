package tempdir

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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

// ForDeleteAllRepositories returns a temporary directory for the given storage. It is not context-scoped but it will get removed eventuall (after MaxAge).
func ForDeleteAllRepositories(locator storage.Locator, storageName string) (string, error) {
	prefix := fmt.Sprintf("%s-repositories.old.%d.", storageName, time.Now().Unix())
	_, path, err := newAsRepository(context.Background(), storageName, prefix, locator)

	return path, err
}

// New returns the path of a new temporary directory for use with the
// repository. The directory is removed with os.RemoveAll when ctx
// expires.
func New(ctx context.Context, repo *gitalypb.Repository, locator storage.Locator) (string, error) {
	_, path, err := NewAsRepository(ctx, repo, locator)
	if err != nil {
		return "", err
	}

	return path, nil
}

// NewAsRepository is the same as New, but it returns a *gitalypb.Repository for the
// created directory as well as the bare path as a string
func NewAsRepository(ctx context.Context, repo *gitalypb.Repository, loc storage.Locator) (*gitalypb.Repository, string, error) {
	return newAsRepository(ctx, repo.StorageName, "repo", loc)
}

func newAsRepository(ctx context.Context, storageName string, prefix string, loc storage.Locator) (*gitalypb.Repository, string, error) {
	storagePath, err := loc.GetStorageByName(storageName)
	if err != nil {
		return nil, "", err
	}

	root := AppendTempDir(storagePath)
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, "", err
	}

	tempDir, err := ioutil.TempDir(root, prefix)
	if err != nil {
		return nil, "", err
	}

	go func() {
		<-ctx.Done()
		os.RemoveAll(tempDir)
	}()

	newAsRepo := &gitalypb.Repository{StorageName: storageName}
	newAsRepo.RelativePath, err = filepath.Rel(storagePath, tempDir)
	return newAsRepo, tempDir, err
}

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

		entry := log.WithFields(log.Fields{
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
		log.Print(dir)
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
