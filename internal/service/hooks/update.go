package hook

import (
	"errors"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) UpdateHook(in *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	if err := validateUpdateHookRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	hookEnv, err := hookRequestEnv(in)
	if err != nil {
		return helper.ErrInternal(err)
	}

	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stderr: p}) })

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	c := exec.Command(gitlabShellHook("update"), string(in.GetRef()), in.GetOldValue(), in.GetNewValue())

	c.Dir = repoPath

	status, err := streamCommandResponse(
		stream.Context(),
		nil,
		stdout, stderr,
		c,
		hookEnv,
	)

	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := stream.SendMsg(&gitalypb.UpdateHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: status},
	}); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateUpdateHookRequest(in *gitalypb.UpdateHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}
