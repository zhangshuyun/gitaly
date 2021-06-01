package hook

import (
	"errors"
	"os/exec"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func validateUpdateHookRequest(in *gitalypb.UpdateHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}

func (s *server) UpdateHook(in *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	if err := validateUpdateHookRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.UpdateHookResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.UpdateHookResponse{Stderr: p})
	})

	if err := s.manager.UpdateHook(
		stream.Context(),
		in.GetRepository(),
		string(in.GetRef()),
		in.GetOldValue(),
		in.GetNewValue(),
		in.GetEnvironmentVariables(),
		stdout,
		stderr,
	); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return updateHookResponse(stream, int32(exitError.ExitCode()))
		}

		return helper.ErrInternal(err)
	}

	return updateHookResponse(stream, 0)
}

func updateHookResponse(stream gitalypb.HookService_UpdateHookServer, code int32) error {
	if err := stream.Send(&gitalypb.UpdateHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: code},
	}); err != nil {
		return helper.ErrInternalf("sending response: %v", err)
	}

	return nil
}
