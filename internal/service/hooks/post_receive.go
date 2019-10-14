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

func postReceiveStdout(p []byte) interface{} {
	return &gitalypb.PostReceiveHookResponse{Stdout: p}
}

func postReceiveStderr(p []byte) interface{} {
	return &gitalypb.PostReceiveHookResponse{Stderr: p}
}

func postReceiveLastResponse(success bool) interface{} {
	return &gitalypb.PostReceiveHookResponse{Success: success}
}

func (s *server) PostReceiveHook(stream gitalypb.HookService_PostReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validatePostReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()

	postReceiveHookPath := filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "hooks", "post-receive")

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	env := append(gitlabshell.Env(),
		fmt.Sprintf("GL_REPO_PATH=%s", repoPath),
		fmt.Sprintf("GL_ID=%s", firstRequest.GetKeyId()),
		fmt.Sprintf("GL_REPOSITORY=%s", firstRequest.GetRepository().GetGlRepository()),
	)

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	if err := streamCommandResponse(
		ctx,
		stream,
		exec.Command(postReceiveHookPath),
		stdin,
		postReceiveStdout,
		postReceiveStderr,
		postReceiveLastResponse,
		env,
	); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validatePostReceiveHookRequest(in *gitalypb.PostReceiveHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}
