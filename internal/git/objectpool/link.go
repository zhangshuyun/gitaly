package objectpool

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git/remote"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// Link will write the relative path to the object pool from the repository that
// is to join the pool. This does not trigger deduplication, which is the
// responsibility of the caller.
func (o *ObjectPool) Link(ctx context.Context, repo *gitalypb.Repository) error {
	altPath, err := git.AlternatesPath(repo)
	if err != nil {
		return err
	}

	relPath, err := o.getRelativeObjectPath(repo)
	if err != nil {
		return err
	}

	remoteName := repo.GetGlRepository()
	for k, v := range map[string]string{
		fmt.Sprintf("remote.%s.url", remoteName):    relPath,
		fmt.Sprintf("remote.%s.fetch", remoteName):  fmt.Sprintf("+refs/*:refs/remotes/%s/*", remoteName),
		fmt.Sprintf("remote.%s.tagOpt", remoteName): "--no-tags",
	} {
		if err := o.setConfig(ctx, k, v); err != nil {
			return err
		}
	}

	expectedContent := filepath.Join(relPath, "objects")

	actualContent, err := ioutil.ReadFile(altPath)
	if err == nil {
		if strings.TrimSuffix(string(actualContent), "\n") == expectedContent {
			return nil
		}

		return fmt.Errorf("unexpected alternates content: %q", actualContent)
	}

	if !os.IsNotExist(err) {
		return err
	}

	tmp, err := ioutil.TempFile(filepath.Dir(altPath), "alternates")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := io.WriteString(tmp, expectedContent); err != nil {
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmp.Name(), altPath)
}

func (o *ObjectPool) getRelativeObjectPath(repo *gitalypb.Repository) (string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	relPath, err := filepath.Rel(filepath.Join(repoPath, "objects"), o.FullPath())
	if err != nil {
		return "", err
	}

	return relPath, nil
}

// LinkedToRepository tests if a repository is linked to an object pool
func (o *ObjectPool) LinkedToRepository(repo *gitalypb.Repository) (bool, error) {
	altPath, err := git.AlternatesPath(repo)
	if err != nil {
		return false, err
	}

	relPath, err := o.getRelativeObjectPath(repo)
	if err != nil {
		return false, err
	}

	if stat, err := os.Stat(altPath); err == nil && stat.Size() > 0 {
		alternatesFile, err := os.Open(altPath)
		if err != nil {
			return false, err
		}
		defer alternatesFile.Close()

		r := bufio.NewReader(alternatesFile)

		b, err := r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return false, fmt.Errorf("reading alternates file: %v", err)
		}

		return string(b) == filepath.Join(relPath, "objects"), nil
	}

	return false, nil
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
	if err := remote.Remove(ctx, o, remoteName); err != nil {
		if present, err2 := remote.Exists(ctx, o, remoteName); err2 != nil || present {
			return err
		}
	}

	altPath, err := git.AlternatesPath(repo)
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
