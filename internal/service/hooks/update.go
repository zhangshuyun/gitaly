package hook

import (
	"errors"
	"fmt"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func validateUpdateHookRequest(in *gitalypb.UpdateHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}

func useGoUpdateHook(env []string) bool {
	useGoHookEnvVarPair := fmt.Sprintf("%s=true", featureflag.GoUpdateHookEnvVar)

	for _, envPair := range env {
		if envPair == useGoHookEnvVarPair {
			return true
		}
	}

	return false
}

func updateHookRuby(in *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stderr: p}) })

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	c := exec.Command(gitlabShellHook("update"), string(in.GetRef()), in.GetOldValue(), in.GetNewValue())
	c.Dir = repoPath

	updateEnv, err := hookRequestEnv(in)
	if err != nil {
		return helper.ErrInternal(err)
	}

	status, err := streamCommandResponse(
		stream.Context(),
		nil,
		stdout, stderr,
		c,
		updateEnv,
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

func (s *server) UpdateHook(in *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	if err := validateUpdateHookRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if !useGoUpdateHook(in.GetEnvironmentVariables()) {
		return updateHookRuby(in, stream)
	}

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}
	executor, err := newCustomHooksExecutor(repoPath, config.Config.Hooks.CustomHooksDir, "update")
	if err != nil {
		return helper.ErrInternal(err)
	}

	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.UpdateHookResponse{Stderr: p}) })

	if err = executor(
		stream.Context(),
		[]string{string(in.GetRef()), in.GetOldValue(), in.GetNewValue()},
		in.GetEnvironmentVariables(),
		nil,
		stdout,
		stderr,
	); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
