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

// Dir is a storage-scoped temporary directory.
type Dir struct {
	path   string
	doneCh chan struct{}
}

// Path returns the absolute path of the temporary directory.
func (d Dir) Path() string {
	return d.path
}

// New returns the path of a new temporary directory for the given storage. The directory is removed
// asynchronously with os.RemoveAll when the context expires.
func New(ctx context.Context, storageName string, locator storage.Locator) (Dir, error) {
	dir, err := newDirectory(ctx, storageName, "repo", locator)
	if err != nil {
		return Dir{}, err
	}

	go dir.cleanupOnDone(ctx)

	return dir, nil
}

// NewWithoutContext returns a temporary directory for the given storage suitable which is not
// storage scoped. The temporary directory will thus not get cleaned up when the context expires,
// but instead when the temporary directory is older than MaxAge.
func NewWithoutContext(storageName string, locator storage.Locator) (Dir, error) {
	prefix := fmt.Sprintf("%s-repositories.old.%d.", storageName, time.Now().Unix())
	return newDirectory(context.Background(), storageName, prefix, locator)
}

// NewRepository is the same as New, but it returns a *gitalypb.Repository for the created directory
// as well as the bare path as a string.
func NewRepository(ctx context.Context, storageName string, locator storage.Locator) (*gitalypb.Repository, Dir, error) {
	storagePath, err := locator.GetStorageByName(storageName)
	if err != nil {
		return nil, Dir{}, err
	}

	dir, err := New(ctx, storageName, locator)
	if err != nil {
		return nil, Dir{}, err
	}

	newRepo := &gitalypb.Repository{StorageName: storageName}
	newRepo.RelativePath, err = filepath.Rel(storagePath, dir.Path())
	if err != nil {
		return nil, Dir{}, err
	}

	return newRepo, dir, nil
}

func newDirectory(ctx context.Context, storageName string, prefix string, loc storage.Locator) (Dir, error) {
	storagePath, err := loc.GetStorageByName(storageName)
	if err != nil {
		return Dir{}, err
	}

	root := AppendTempDir(storagePath)
	if err := os.MkdirAll(root, 0700); err != nil {
		return Dir{}, err
	}

	tempDir, err := ioutil.TempDir(root, prefix)
	if err != nil {
		return Dir{}, err
	}

	return Dir{
		path:   tempDir,
		doneCh: make(chan struct{}),
	}, err
}

func (d Dir) cleanupOnDone(ctx context.Context) {
	<-ctx.Done()
	os.RemoveAll(d.Path())
	close(d.doneCh)
}

// WaitForCleanup waits until the temporary directory got removed via the asynchronous cleanupOnDone
// call. This is mainly intended for use in tests.
func (d Dir) WaitForCleanup() {
	<-d.doneCh
}
