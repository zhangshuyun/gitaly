package tempdir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
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
	return NewWithPrefix(ctx, storageName, "repo", locator)
}

// NewWithPrefix returns the path of a new temporary directory for the given storage with a specific
// prefix used to create the temporary directory's name. The directory is removed asynchronously
// with os.RemoveAll when the context expires.
func NewWithPrefix(ctx context.Context, storageName, prefix string, locator storage.Locator) (Dir, error) {
	dir, err := newDirectory(ctx, storageName, prefix, locator)
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
	root, err := loc.TempDir(storageName)
	if err != nil {
		return Dir{}, fmt.Errorf("temp directory: %w", err)
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return Dir{}, err
	}

	tempDir, err := os.MkdirTemp(root, prefix)
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
	if err := os.RemoveAll(d.Path()); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Errorf("failed to cleanup temp dir %q", d.path)
	}
	close(d.doneCh)
}

// WaitForCleanup waits until the temporary directory got removed via the asynchronous cleanupOnDone
// call. This is mainly intended for use in tests.
func (d Dir) WaitForCleanup() {
	<-d.doneCh
}
