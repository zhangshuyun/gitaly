package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func removeOriginInRepo(ctx context.Context, repository *gitalypb.Repository) error {
	cmd, err := git.SafeCmd(ctx, repository, nil, git.SubCmd{Name: "remote", Args: []string{"remove", "origin"}})

	if err != nil {
		return fmt.Errorf("remote cmd start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("remote cmd wait: %v", err)
	}

	return nil
}
