package hook

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (m *GitLabHookManager) UpdateHook(ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	executor, err := m.newCustomHooksExecutor(repo, "update")
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err = executor(
		ctx,
		[]string{ref, oldValue, newValue},
		env,
		nil,
		stdout,
		stderr,
	); err != nil {
		return err
	}

	return nil
}
