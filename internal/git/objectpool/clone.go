package objectpool

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

// Clone a repository to a pool, without setting the alternates, is not the
// resposibility of this function.
func (o *ObjectPool) clone(ctx context.Context, repo *gitalypb.Repository) error {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return err
	}

	cmd, err := git.SafeCmdWithoutRepo(ctx, git.CmdStream{}, nil,
		git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--local"},
			},
			Args: []string{repoPath, o.FullPath()},
		},
	)
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func (o *ObjectPool) removeHooksDir() error {
	return os.RemoveAll(filepath.Join(o.FullPath(), "hooks"))
}
