package hook

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
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

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	if err := s.manager.ReferenceTransactionHook(
		stream.Context(),
		request.GetEnvironmentVariables(),
		stdin,
	); err != nil {
		return helper.ErrInternalf("error voting on transaction: %v", err)
	}

	if err := stream.Send(&gitalypb.ReferenceTransactionHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: 0},
	}); err != nil {
		return helper.ErrInternalf("sending response: %v", err)
	}

	return nil
}
