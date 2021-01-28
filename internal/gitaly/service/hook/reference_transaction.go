package hook

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func validateReferenceTransactionHookRequest(in *gitalypb.ReferenceTransactionHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}

func (s *server) ReferenceTransactionHook(stream gitalypb.HookService_ReferenceTransactionHookServer) error {
	request, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receiving first request: %w", err)
	}

	if err := validateReferenceTransactionHookRequest(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	var state hook.ReferenceTransactionState
	switch request.State {
	case gitalypb.ReferenceTransactionHookRequest_PREPARED:
		state = hook.ReferenceTransactionPrepared
	case gitalypb.ReferenceTransactionHookRequest_COMMITTED:
		state = hook.ReferenceTransactionCommitted
	case gitalypb.ReferenceTransactionHookRequest_ABORTED:
		state = hook.ReferenceTransactionAborted
	default:
		return helper.ErrInvalidArgument(errors.New("invalid hook state"))
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	if err := s.manager.ReferenceTransactionHook(
		stream.Context(),
		state,
		request.GetEnvironmentVariables(),
		stdin,
	); err != nil {
		return helper.ErrInternalf("reference-transaction hook: %v", err)
	}

	if err := stream.Send(&gitalypb.ReferenceTransactionHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: 0},
	}); err != nil {
		return helper.ErrInternalf("sending response: %v", err)
	}

	return nil
}
