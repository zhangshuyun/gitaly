package hook

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func updateHookStdout(p []byte) interface{} {
	return &gitalypb.UpdateHookResponse{Stdout: p}
}

func updateHookStderr(p []byte) interface{} {
	return &gitalypb.UpdateHookResponse{Stderr: p}
}

func updateHookLastResponse(success bool) interface{} {
	return &gitalypb.UpdateHookResponse{Success: success}
}

func (s *server) UpdateHook(in *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	if err := validateUpdateHookRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	ctx := stream.Context()

	updateHookPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "hooks", "update")

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	env := append(gitlabshell.Env(), []string{
		fmt.Sprintf("GL_ID=%s", in.GetKeyId()),
		fmt.Sprintf("GL_REPO_PATH=%s", repoPath),
	}...)

	if err := streamCommandResponse(
		ctx,
		stream,
		exec.Command(updateHookPath, string(in.GetRef()), in.GetOldValue(), in.GetNewValue()),
		nil,
		updateHookStdout,
		updateHookStderr,
		updateHookLastResponse,
		env,
	); err != nil {
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
