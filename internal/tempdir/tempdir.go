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
