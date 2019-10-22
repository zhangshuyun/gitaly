package objectpool

import (
	"bufio"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
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

func repackPool(ctx context.Context, pool repository.GitRepo) error {
	globalOpts := []git.Option{
		git.ValueFlag{"-c", "pack.island=" + sourceRefNamespace + "/heads"},
		git.ValueFlag{"-c", "pack.island=" + sourceRefNamespace + "/tags"},
		git.ValueFlag{"-c", "pack.writeBitmapHashCache=true"},
	}

	repackCmd, err := git.SafeCmd(ctx, pool, globalOpts, git.SubCmd{
		Name: "repack",
		Flags: []git.Option{
			git.Flag{"-ad"},
			git.Flag{"--keep-unreachable"},   // Prevent data loss
			git.Flag{"--delta-islands"},      // Performance
			git.Flag{"--write-bitmap-index"}, // Performance
		},
	})
	if err != nil {
		return err
	}

	if err := repackCmd.Wait(); err != nil {
		return err
	}

	return nil
}
