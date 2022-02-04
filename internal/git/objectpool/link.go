package objectpool

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
)

// Link will link the given repository to the object pool. This is done by writing the object pool's
// path relative to the repository into the repository's "alternates" file. This does not trigger
// deduplication, which is the responsibility of the caller.
func (o *ObjectPool) Link(ctx context.Context, repo *localrepo.Repo) (returnedErr error) {
	altPath, err := repo.InfoAlternatesPath()
	if err != nil {
		return err
	}

	expectedRelPath, err := o.getRelativeObjectPath(repo)
	if err != nil {
		return err
	}

	linked, err := o.LinkedToRepository(repo)
	if err != nil {
		return err
	}

	if linked {
		return nil
	}

	alternatesWriter, err := safe.NewLockingFileWriter(altPath)
	if err != nil {
		return fmt.Errorf("creating alternates writer: %w", err)
	}
	defer func() {
		if err := alternatesWriter.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("closing alternates writer: %w", err)
		}
	}()

	if _, err := io.WriteString(alternatesWriter, expectedRelPath); err != nil {
		return fmt.Errorf("writing alternates: %w", err)
	}

	if err := transaction.CommitLockedFile(ctx, o.txManager, alternatesWriter); err != nil {
		return fmt.Errorf("committing alternates: %w", err)
	}

	return o.removeMemberBitmaps(repo)
}

// removeMemberBitmaps removes packfile bitmaps from the member
// repository that just joined the pool. If Git finds two packfiles with
// bitmaps it will print a warning, which is visible to the end user
// during a Git clone. Our goal is to avoid that warning. In normal
// operation, the next 'git gc' or 'git repack -ad' on the member
// repository will remove its local bitmap file. In other words the
// situation we eventually converge to is that the pool may have a bitmap
// but none of its members will. With removeMemberBitmaps we try to
// change "eventually" to "immediately", so that users won't see the
// warning. https://gitlab.com/gitlab-org/gitaly/issues/1728
func (o *ObjectPool) removeMemberBitmaps(repo *localrepo.Repo) error {
	poolPath, err := o.Repo.Path()
	if err != nil {
		return err
	}

	poolBitmaps, err := getBitmaps(poolPath)
	if err != nil {
		return err
	}
	if len(poolBitmaps) == 0 {
		return nil
	}

	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	memberBitmaps, err := getBitmaps(repoPath)
	if err != nil {
		return err
	}
	if len(memberBitmaps) == 0 {
		return nil
	}

	for _, bitmap := range memberBitmaps {
		if err := os.Remove(bitmap); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func getBitmaps(repoPath string) ([]string, error) {
	packDir := filepath.Join(repoPath, "objects/pack")
	entries, err := os.ReadDir(packDir)
	if err != nil {
		return nil, err
	}

	var bitmaps []string
	for _, entry := range entries {
		if name := entry.Name(); strings.HasSuffix(name, ".bitmap") && strings.HasPrefix(name, "pack-") {
			bitmaps = append(bitmaps, filepath.Join(packDir, name))
		}
	}

	return bitmaps, nil
}

func (o *ObjectPool) getRelativeObjectPath(repo *localrepo.Repo) (string, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return "", err
	}

	relPath, err := filepath.Rel(filepath.Join(repoPath, "objects"), o.FullPath())
	if err != nil {
		return "", err
	}

	return filepath.Join(relPath, "objects"), nil
}

// LinkedToRepository tests if a repository is linked to an object pool
func (o *ObjectPool) LinkedToRepository(repo *localrepo.Repo) (bool, error) {
	relPath, err := getAlternateObjectDir(repo)
	if err != nil {
		if err == ErrAlternateObjectDirNotExist {
			return false, nil
		}
		return false, err
	}

	expectedRelPath, err := o.getRelativeObjectPath(repo)
	if err != nil {
		return false, err
	}

	if relPath == expectedRelPath {
		return true, nil
	}

	if filepath.Clean(relPath) != filepath.Join(o.FullPath(), "objects") {
		return false, fmt.Errorf("unexpected alternates content: %q", relPath)
	}

	return false, nil
}
