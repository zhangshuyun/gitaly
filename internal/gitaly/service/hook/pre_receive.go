package hook

import (
	"errors"
	"fmt"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
)

type hookRequest interface {
	GetEnvironmentVariables() []string
	GetRepository() *gitalypb.Repository
}

type prePostRequest interface {
	hookRequest
	GetGitPushOptions() []string
}

func hookRequestEnv(req hookRequest) ([]string, error) {
	gitlabshellEnv, err := gitlabshell.Env()
	if err != nil {
		return nil, err
	}
	return append(gitlabshellEnv, req.GetEnvironmentVariables()...), nil
}

func preReceiveEnv(req prePostRequest) ([]string, error) {
	env, err := hookRequestEnv(req)
	if err != nil {
		return nil, err
	}

	env = append(env, hooks.GitPushOptions(req.GetGitPushOptions())...)

	return env, nil
}

func (s *server) PreReceiveHook(stream gitalypb.HookService_PreReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receiving first request: %w", err)
	}

	if err := validatePreReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	reqEnvVars := firstRequest.GetEnvironmentVariables()
	repository := firstRequest.GetRepository()

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stderr: p}) })

	env, err := preReceiveEnv(firstRequest)
	if err != nil {
		return fmt.Errorf("getting env vars from request: %v", err)
	}

	if err := s.manager.PreReceiveHook(
		stream.Context(),
		repository,
		append(env, reqEnvVars...),
		stdin,
		stdout,
		stderr,
	); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return preReceiveHookResponse(stream, int32(exitError.ExitCode()), "")
		}

		return preReceiveHookResponse(stream, 1, fmt.Sprintf("%s", err))
	}

	return preReceiveHookResponse(stream, 0, "")
}

func validatePreReceiveHookRequest(in *gitalypb.PreReceiveHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}

func preReceiveHookResponse(stream gitalypb.HookService_PreReceiveHookServer, code int32, stderr string) error {
	if err := stream.Send(&gitalypb.PreReceiveHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: code},
		Stderr:     []byte(stderr),
	}); err != nil {
		return helper.ErrInternalf("sending response: %v", err)
	}

	return nil
}
