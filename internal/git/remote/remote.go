package remote

import (
	"bufio"
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

//Remove removes the remote from repository
func Remove(ctx context.Context, cfg config.Cfg, repo repository.GitRepo, name string) error {
	cmd, err := git.NewCommand(ctx, repo, nil, git.SubSubCmd{
		Name:   "remote",
		Action: "remove",
		Args:   []string{name},
	}, git.WithRefTxHook(ctx, repo, cfg))
	if err != nil {
		return err
	}

	return cmd.Wait()
}

// Exists will always return a boolean value, but should only be depended on
// when the error value is nil
func Exists(ctx context.Context, cfg config.Cfg, repo repository.GitRepo, name string) (bool, error) {
	cmd, err := git.NewCommand(ctx, repo, nil,
		git.SubCmd{Name: "remote"},
		git.WithRefTxHook(ctx, repo, cfg),
	)
	if err != nil {
		return false, err
	}

	found := false
	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		if scanner.Text() == name {
			found = true
			break
		}
	}

	return found, cmd.Wait()
}
