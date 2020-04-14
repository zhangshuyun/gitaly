package hook

import (
	"errors"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) PostReceiveHook(stream gitalypb.HookService_PostReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validatePostReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	hookEnv, err := hookRequestEnv(firstRequest)
	if err != nil {
		return helper.ErrInternal(err)
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PostReceiveHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PostReceiveHookResponse{Stderr: p}) })

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	c := exec.Command(gitlabShellHook("post-receive"))
	c.Dir = repoPath

	status, err := streamCommandResponse(
		stream.Context(),
		stdin,
		stdout, stderr,
		c,
		append(hookEnv, hooks.GitPushOptions(firstRequest.GetGitPushOptions())...),
	)

	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := stream.SendMsg(&gitalypb.PostReceiveHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: status},
	}); err != nil {
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
