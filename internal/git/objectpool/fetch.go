package objectpool

import (
	"bufio"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	sourceRemote       = "origin"
	sourceRefNamespace = "refs/remotes/" + sourceRemote
)

// FetchFromOrigin initializes the pool and fetches the objects from its origin repository
func (o *ObjectPool) FetchFromOrigin(ctx context.Context, origin *gitalypb.Repository) error {
	if err := o.Init(ctx); err != nil {
		return err
	}

	originPath, err := helper.GetPath(origin)

	if err != nil {
		return err
	}

	getRemotes, err := git.Command(ctx, o, "remote")
	if err != nil {
		return err
	}

	remoteReader := bufio.NewScanner(getRemotes)

	var originExists bool
	for remoteReader.Scan() {
		if remoteReader.Text() == sourceRemote {
			originExists = true
		}
	}
	if err := getRemotes.Wait(); err != nil {
		return err
	}

	var setOriginCmd *command.Command
	if originExists {
		setOriginCmd, err = git.Command(ctx, o, "remote", "set-url", sourceRemote, originPath)
		if err != nil {
			return err
		}
	} else {
		setOriginCmd, err = git.Command(ctx, o, "remote", "add", sourceRemote, originPath)
		if err != nil {
			return err
		}
	}

	if err := setOriginCmd.Wait(); err != nil {
		return err
	}

	refSpec := fmt.Sprintf("+refs/*:%s/*", sourceRefNamespace)
	fetchCmd, err := git.Command(ctx, o, "fetch", "--quiet", sourceRemote, refSpec)
	if err != nil {
		return err
	}

	if err := fetchCmd.Wait(); err != nil {
		return err
	}

	if err := removeDanglingRefs(ctx, o); err != nil {
		return err
	}

	packRefs, err := git.Command(ctx, o, "pack-refs", "--all")
	if err != nil {
		return err
	}
	if err := packRefs.Wait(); err != nil {
		return err
	}

	return repackPool(ctx, o)
}

const danglingObjectNamespace = "refs/dangling"

// In https://gitlab.com/gitlab-org/gitaly/merge_requests/1297 we
// introduced a mechanism to protect dangling objects in a pool from
// being removed by a manual 'git prune' command. It turned out that
// created way too many refs, so now we want to make sure no such refs
// are left behind.
func removeDanglingRefs(ctx context.Context, repo repository.GitRepo) error {
	forEachRef, err := git.Command(ctx, repo, "for-each-ref", "--format=%(refname)", danglingObjectNamespace)
	if err != nil {
		return err
	}

	updater, err := updateref.New(ctx, repo)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(forEachRef)
	for scanner.Scan() {
		if err := updater.Delete(scanner.Text()); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := forEachRef.Wait(); err != nil {
		return fmt.Errorf("git for-each-ref: %v", err)
	}

	return updater.Wait()
}

func repackPool(ctx context.Context, pool repository.GitRepo) error {
	repackArgs := []string{
		"-c", "pack.island=" + sourceRefNamespace + "/heads",
		"-c", "pack.island=" + sourceRefNamespace + "/tags",
		"-c", "pack.writeBitmapHashCache=true",
		"repack", "-aidb",
	}
	repackCmd, err := git.Command(ctx, pool, repackArgs...)
	if err != nil {
		return err
	}

	if err := repackCmd.Wait(); err != nil {
		return err
	}

	return nil
}
