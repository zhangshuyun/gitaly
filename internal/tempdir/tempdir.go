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
	return newDirectory(context.Background(), storageName, prefix, locator)
}

// New returns the path of a new temporary directory for use with the repository. The directory is
// removed with os.RemoveAll when the context expires.
func New(ctx context.Context, repo *gitalypb.Repository, locator storage.Locator) (string, error) {
	return newDirectory(ctx, repo.StorageName, "repo", locator)
}

// NewAsRepository is the same as New, but it returns a *gitalypb.Repository for the created
// directory as well as the bare path as a string
func NewAsRepository(ctx context.Context, repo *gitalypb.Repository, locator storage.Locator) (*gitalypb.Repository, string, error) {
	storagePath, err := locator.GetStorageByName(repo.StorageName)
	if err != nil {
		return nil, "", err
	}

	path, err := New(ctx, repo, locator)
	if err != nil {
		return nil, "", err
	}

	newRepo := &gitalypb.Repository{StorageName: repo.StorageName}
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
