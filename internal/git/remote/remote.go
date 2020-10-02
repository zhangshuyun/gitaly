package remote

import (
	"bufio"
	"context"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/repository"
)

//Remove removes the remote from repository
func Remove(ctx context.Context, repo repository.GitRepo, name string) error {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name:  "remote",
		Flags: []git.Option{git.SubSubCmd{Name: "remove"}},
		Args:  []string{name},
	})
	if err != nil {
		return err
	}

	return cmd.Wait()
}

// Exists will always return a boolean value, but should only be depended on
// when the error value is nil
func Exists(ctx context.Context, repo repository.GitRepo, name string) (bool, error) {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{Name: "remote"})
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
