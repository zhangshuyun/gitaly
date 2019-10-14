package hook

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/streamio"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func preReceiveStdout(p []byte) interface{} {
	return &gitalypb.PreReceiveHookResponse{Stdout: p}
}

func preReceiveStderr(p []byte) interface{} {
	return &gitalypb.PreReceiveHookResponse{Stderr: p}
}

func preReceiveLastResponse(success bool) interface{} {
	return &gitalypb.PreReceiveHookResponse{Success: success}
}

func (s *server) PreReceiveHook(stream gitalypb.HookService_PreReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validatePreReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	ctx := stream.Context()

	preReceiveHookPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "hooks", "pre-receive")

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	env := append(gitlabshell.Env(),
		fmt.Sprintf("GL_ID=%s", firstRequest.GetKeyId()),
		fmt.Sprintf("GL_PROTOCOL=%s", firstRequest.GetProtocol()),
		fmt.Sprintf("GL_REPO_PATH=%s", repoPath),
		fmt.Sprintf("GL_REPOSITORY=%s", firstRequest.GetRepository().GetGlRepository()),
	)

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	if err := streamCommandResponse(
		ctx,
		stream,
		exec.Command(preReceiveHookPath),
		stdin,
		preReceiveStdout,
		preReceiveStderr,
		preReceiveLastResponse,
		env,
	); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validatePreReceiveHookRequest(in *gitalypb.PreReceiveHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}
