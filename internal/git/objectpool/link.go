package objectpool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Link will write the relative path to the object pool from the repository that
// is to join the pool. This does not trigger deduplication, which is the
// responsibility of the caller.
func (o *ObjectPool) Link(ctx context.Context, repo *gitalypb.Repository) error {
	altPath, err := o.locator.InfoAlternatesPath(repo)
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

	tmp, err := ioutil.TempFile(filepath.Dir(altPath), "alternates")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := io.WriteString(tmp, expectedRelPath); err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmp.Name(), altPath); err != nil {
		return err
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
func (o *ObjectPool) removeMemberBitmaps(repo repository.GitRepo) error {
	poolPath, err := o.locator.GetPath(o)
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

	repoPath, err := o.locator.GetPath(repo)
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
	entries, err := ioutil.ReadDir(packDir)
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

func (o *ObjectPool) getRelativeObjectPath(repo *gitalypb.Repository) (string, error) {
	repoPath, err := o.locator.GetRepoPath(repo)
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
func (o *ObjectPool) LinkedToRepository(repo *gitalypb.Repository) (bool, error) {
	relPath, err := getAlternateObjectDir(o.locator, repo)
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

// Unlink removes the remote from the object pool
func (o *ObjectPool) Unlink(ctx context.Context, repo *gitalypb.Repository) error {
	if !o.Exists() {
		return errors.New("pool does not exist")
	}

	remote := o.poolRepo.Remote()

	// We need to use removeRemote, and can't leverage `git config --remove-section`
	// as the latter doesn't clean up refs
	remoteName := repo.GetGlRepository()
	if err := remote.Remove(ctx, remoteName); err != nil {
		if present, err2 := remote.Exists(ctx, remoteName); err2 != nil || present {
			return err
		}
	}

	return nil
}
