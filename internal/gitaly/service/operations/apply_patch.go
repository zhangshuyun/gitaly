package operations

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserApplyPatch(stream gitalypb.OperationService_UserApplyPatchServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "UserApplyPatch: empty UserApplyPatch_Header")
	}

	if err := validateUserApplyPatchHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "UserApplyPatch: %v", err)
	}

	requestCtx := stream.Context()
	rubyClient, err := s.ruby.OperationServiceClient(requestCtx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(requestCtx, s.locator, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := rubyClient.UserApplyPatch(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})
	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateUserApplyPatchHeader(header *gitalypb.UserApplyPatchRequest_Header) error {
	if header.GetRepository() == nil {
		return fmt.Errorf("missing Repository")
	}

	if header.GetUser() == nil {
		return fmt.Errorf("missing User")
	}

	if len(header.GetTargetBranch()) == 0 {
		return fmt.Errorf("missing Branch")
	}

	return nil
}
