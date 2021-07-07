package tempdir

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// New returns the path of a new temporary directory for the given storage. The directory is removed
// asynchronously with os.RemoveAll when the context expires.
func New(ctx context.Context, storageName string, locator storage.Locator) (string, error) {
	return newDirectory(ctx, storageName, "repo", locator)
}

// NewWithoutContext returns a temporary directory for the given storage suitable which is not
// storage scoped. The temporary directory will thus not get cleaned up when the context expires,
// but instead when the temporary directory is older than MaxAge.
func NewWithoutContext(storageName string, locator storage.Locator) (string, error) {
	prefix := fmt.Sprintf("%s-repositories.old.%d.", storageName, time.Now().Unix())
	return newDirectory(context.Background(), storageName, prefix, locator)
}

// NewRepository is the same as New, but it returns a *gitalypb.Repository for the created directory
// as well as the bare path as a string.
func NewRepository(ctx context.Context, storageName string, locator storage.Locator) (*gitalypb.Repository, string, error) {
	storagePath, err := locator.GetStorageByName(storageName)
	if err != nil {
		return nil, "", err
	}

	path, err := New(ctx, storageName, locator)
	if err != nil {
		return nil, "", err
	}

	newRepo := &gitalypb.Repository{StorageName: storageName}
	newRepo.RelativePath, err = filepath.Rel(storagePath, path)
	if err != nil {
		return nil, "", err
	}

	return newRepo, path, nil
}

func newDirectory(ctx context.Context, storageName string, prefix string, loc storage.Locator) (string, error) {
	storagePath, err := loc.GetStorageByName(storageName)
	if err != nil {
		return "", err
	}

	root := AppendTempDir(storagePath)
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", err
	}

	tempDir, err := ioutil.TempDir(root, prefix)
	if err != nil {
		return "", err
	}

	go func() {
		<-ctx.Done()
		os.RemoveAll(tempDir)
	}()

	return tempDir, err
}
