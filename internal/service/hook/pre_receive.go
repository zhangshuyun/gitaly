package hook

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) PreReceive(ctx context.Context, in *gitalypb.PreReceiveHookRequest) (*gitalypb.PreReceiveHookResponse, error) {
	prereceiveHook := fmt.Sprintf("%s/pre-receive", hooks.Path())

	var stdin bytes.Buffer
	for _, ref := range in.GetRefs() {
		stdin.WriteString(fmt.Sprintf("%s\n", ref))
	}

	env := []string{
		fmt.Sprintf("GL_ID=%s", in.GetKeyId()),
		fmt.Sprintf("GL_PROTOCOL=%s", in.GetProtocol()),
		fmt.Sprintf("GL_REPO_PATH=%s", in.GetRepoPath()),
		fmt.Sprintf("GL_REPOSITORY=%s", in.GetGlRepository()),
	}

	cmd, err := command.New(ctx, exec.Command(prereceiveHook), &stdin, nil, nil, env...)
	if err != nil {
		return &gitalypb.PreReceiveHookResponse{}, helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return &gitalypb.PreReceiveHookResponse{}, helper.ErrInternal(err)
	}

	return &gitalypb.PreReceiveHookResponse{}, nil
}
