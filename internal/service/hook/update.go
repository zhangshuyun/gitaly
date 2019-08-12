package hook

import (
	"context"
	"fmt"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) Update(ctx context.Context, in *gitalypb.UpdateHookRequest) (*gitalypb.UpdateHookResponse, error) {
	updateHook := fmt.Sprintf("%s/pre-receive", hooks.Path())

	env := []string{
		fmt.Sprintf("GL_ID=%s", in.GetKeyId()),
		fmt.Sprintf("GL_REPO_PATH=%s", in.GetRepoPath()),
	}

	cmd, err := command.New(ctx, exec.Command(updateHook, in.GetRef(), in.GetOldValue(), in.GetNewValue()), nil, nil, nil, env...)
	if err != nil {
		return &gitalypb.UpdateHookResponse{}, helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return &gitalypb.UpdateHookResponse{}, helper.ErrInternal(err)
	}

	return &gitalypb.UpdateHookResponse{}, nil
}
