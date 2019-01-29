package objectpool

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// Link will write the relative path to the object pool from the repository that
// is to join the pool. This does not trigger deduplication, which is the
// responsibility of the caller.
func (o *ObjectPool) Link(ctx context.Context, repo *gitalypb.Repository) error {
	altPath, err := alternatesPath(repo)
	if err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return err
	}

	relPath, err := filepath.Rel(filepath.Join(repoPath, "objects"), o.FullPath())
	if err != nil {
		return err
	}

	remoteName := repo.GetGlRepository()
	for k, v := range map[string]string{
		fmt.Sprintf("remote.%s.url", remoteName):    relPath,
		fmt.Sprintf("remote.%s.fetch", remoteName):  fmt.Sprintf("+refs/*:refs/remotes/%s/*", remoteName),
		fmt.Sprintf("remote.%s.tagOpt", remoteName): "--no-tags",
		"pack.island": fmt.Sprintf("refs/remotes/%s", remoteName),
	} {
		if err := o.setConfig(ctx, k, v); err != nil {
			return err
		}
	}

	return ioutil.WriteFile(altPath, []byte(filepath.Join(relPath, "objects")), 0644)
}

// Unlink removes the alternates file, so Git won't look there anymore
// It removes the remote from the object pool too,
func (o *ObjectPool) Unlink(ctx context.Context, repo *gitalypb.Repository) error {
	if !o.Exists() {
		return nil
	}

	// We need to use removeRemote, and can't leverage `git config --remove-section`
	// as the latter doesn't clean up refs
	remoteName := repo.GetGlRepository()
	if err := o.removeRemote(ctx, remoteName); err != nil {
		if present, err2 := o.hasRemote(ctx, remoteName); err2 != nil || present {
			return err
		}
	}

	altPath, err := alternatesPath(repo)
	if err != nil {
		return err
	}

	return os.RemoveAll(altPath)
}

// Config options setting will leak the key value pairs in the logs. This makes
// this function not suitable for general usage, and scoped to this package.
// To be corrected in: https://gitlab.com/gitlab-org/gitaly/issues/1430
func (o *ObjectPool) setConfig(ctx context.Context, key, value string) error {
	cmd, err := git.Command(ctx, o, "config", key, value)
	if err != nil {
		return err
	}

	return cmd.Wait()
}
