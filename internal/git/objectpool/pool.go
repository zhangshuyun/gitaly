package objectpool

import (
	"context"
	"fmt"
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/remote"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// ObjectPool are a way to dedup objects between repositories, where the objects
// live in a pool in a distinct repository which is used as an alternate object
// store for other repositories.
type ObjectPool struct {
	storageName string
	storagePath string

	relativePath string
}

// NewObjectPool will initialize the object with the required data on the storage
// shard. If the shard cannot be found, this function returns an error
func NewObjectPool(storageName, relativePath string) (pool *ObjectPool, err error) {
	storagePath, err := helper.GetStorageByName(storageName)
	if err != nil {
		return nil, err
	}

	return &ObjectPool{storageName, storagePath, relativePath}, nil
}

// GetGitAlternateObjectDirectories for object pools are empty, given pools are
// never a member of another pool, nor do they share Alternate objects with other
// repositories which the pool doesn't contain itself
func (o *ObjectPool) GetGitAlternateObjectDirectories() []string {
	return []string{}
}

// GetGitObjectDirectory satisfies the repository.GitRepo interface, but is not
// used for ObjectPools
func (o *ObjectPool) GetGitObjectDirectory() string {
	return ""
}

// Exists will return true if the pool path exists and is a directory
func (o *ObjectPool) Exists() bool {
	fi, err := os.Stat(o.FullPath())
	if os.IsNotExist(err) || err != nil {
		return false
	}

	return fi.IsDir()
}

// IsValid checks if a repository exists, and if its valid.
func (o *ObjectPool) IsValid() bool {
	if !o.Exists() {
		return false
	}

	return helper.IsGitDirectory(o.FullPath())
}

// Create will create a pool for a repository and pull the required data to this
// pool. `repo` that is passed also joins the repository.
func (o *ObjectPool) Create(ctx context.Context, repo *gitalypb.Repository) (err error) {
	if err := os.MkdirAll(path.Dir(o.FullPath()), 0755); err != nil {
		return err
	}

	if err := o.clone(ctx, repo); err != nil {
		return fmt.Errorf("clone: %v", err)
	}

	if err := o.removeHooksDir(); err != nil {
		return fmt.Errorf("remove hooks: %v", err)
	}

	if err := remote.Remove(ctx, o, "origin"); err != nil {
		return fmt.Errorf("remove origin: %v", err)
	}

	if err := o.removeRefs(ctx); err != nil {
		return fmt.Errorf("remove refs: %v", err)
	}

	if err := o.setConfig(ctx, "gc.auto", "0"); err != nil {
		return fmt.Errorf("config gc.auto: %v", err)
	}

	return nil
}

// Remove will remove the pool, and all its contents without preparing and/or
// updating the repositories depending on this object pool
// Subdirectories will remain to exist, and will never be cleaned up, even when
// these are empty.
func (o *ObjectPool) Remove(ctx context.Context) (err error) {
	return os.RemoveAll(o.FullPath())
}
