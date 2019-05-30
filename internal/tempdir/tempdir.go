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
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/housekeeping"
)

const (
	// GitalyDataPrefix is the top-level directory we use to store system
	// (non-user) data. We need to be careful that this path does not clash
	// with any directory name that could be provided by a user. The '+'
	// character is not allowed in GitLab namespaces or repositories.
	GitalyDataPrefix = "+gitaly"

	// TmpRootPrefix is the directory in which we store temporary
	// directories.
	TmpRootPrefix = GitalyDataPrefix + "/tmp"

	// CachePrefix is the directory where we store ephemeral disk-based caches
	// that can be removed at anytime.
	CachePrefix = GitalyDataPrefix + "/cache"

	// MaxAge is used by ForDeleteAllRepositories. It is also a fallback
	// for the context-scoped temporary directories, to ensure they get
	// cleaned up if the cleanup at the end of the context failed to run.
	MaxAge = 7 * 24 * time.Hour
)

// ForDeleteAllRepositories returns a temporary directory for the given storage. It is not context-scoped but it will get removed eventuall (after MaxAge).
func ForDeleteAllRepositories(storageName string) (string, error) {
	prefix := fmt.Sprintf("%s-repositories.old.%d.", storageName, time.Now().Unix())
	_, path, err := newAsRepository(context.Background(), storageName, prefix)

	return path, err
}

// New returns the path of a new temporary directory for use with the
// repository. The directory is removed with os.RemoveAll when ctx
// expires.
func New(ctx context.Context, repo *gitalypb.Repository) (string, error) {
	_, path, err := NewAsRepository(ctx, repo)
	if err != nil {
		return "", err
	}

	return path, nil
}

// NewAsRepository is the same as New, but it returns a *gitalypb.Repository for the
// created directory as well as the bare path as a string
func NewAsRepository(ctx context.Context, repo *gitalypb.Repository) (*gitalypb.Repository, string, error) {
	return newAsRepository(ctx, repo.StorageName, "repo")
}

func newAsRepository(ctx context.Context, storageName string, prefix string) (*gitalypb.Repository, string, error) {
	storageDir, err := helper.GetStorageByName(storageName)
	if err != nil {
		return nil, "", err
	}

	root := tmpRoot(storageDir)
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
	newAsRepo.RelativePath, err = filepath.Rel(storageDir, tempDir)
	return newAsRepo, tempDir, err
}

func tmpRoot(storageRoot string) string {
	return filepath.Join(storageRoot, TmpRootPrefix)
}

// StartCleaning starts tempdir cleanup goroutines.
func StartCleaning() {
	for _, st := range config.Config.Storages {
		go func(name string, dir string) {
			start := time.Now()
			err := clean(tmpRoot(dir))

			entry := log.WithFields(log.Fields{
				"time_ms": int(1000 * time.Since(start).Seconds()),
				"storage": name,
			})
			if err != nil {
				entry = entry.WithError(err)
			}
			entry.Info("finished tempdir cleaner walk")

			time.Sleep(1 * time.Hour)
		}(st.Name, st.Path)
	}
}

type invalidCleanRoot string

func clean(dir string) error {
	// If we start "cleaning up" the wrong directory we may delete user data
	// which is Really Bad.
	if !strings.HasSuffix(dir, TmpRootPrefix) {
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
		if err := housekeeping.FixDirectoryPermissions(fullPath); err != nil {
			return err
		}

		if err := os.RemoveAll(fullPath); err != nil {
			return err
		}
	}

	return nil
}
