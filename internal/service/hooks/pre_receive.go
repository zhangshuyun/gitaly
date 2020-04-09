package hook

import (
	"errors"
	"io/ioutil"
	"os/exec"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

type hookRequest interface {
	GetEnvironmentVariables() []string
	GetRepository() *gitalypb.Repository
}

func hookRequestEnv(req hookRequest) []string {
	return append(gitlabshell.Env(), req.GetEnvironmentVariables()...)
}

func preReceiveEnv(req hookRequest) ([]string, error) {
	_, env, err := alternates.PathAndEnv(req.GetRepository())
	if err != nil {
		return nil, err
	}
	return append(hookRequestEnv(req), env...), nil
}

func gitlabShellHook(hookName string) string {
	return filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "hooks", hookName)
}

func (s *server) PreReceiveHook(stream gitalypb.HookService_PreReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validatePreReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stderr: p}) })

	env, err := preReceiveEnv(firstRequest)
	if err != nil {
		return helper.ErrInternal(err)
	}

	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternal(err)
	}

	ok, msg, err := s.internalClient.checkAccess(repoPath, string(changes), env)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if !ok {
		if err := stream.SendMsg(&gitalypb.PreReceiveHookResponse{
			ExitStatus: &gitalypb.ExitStatus{Value: 1},
			Stderr:     []byte(msg),
		}); err != nil {
			return helper.ErrInternal(err)
		}
	}

	c := exec.Command(gitlabShellHook("pre-receive"))
	c.Dir = repoPath

	status, err := streamCommandResponse(
		stream.Context(),
		nil,
		stdout, stderr,
		c,
		env,
	)

	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := stream.SendMsg(&gitalypb.PreReceiveHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: status},
	}); err != nil {
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
