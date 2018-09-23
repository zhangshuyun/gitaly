package operations

import (
	"fmt"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCommitFiles(stream pb.OperationService_UserCommitFilesServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "UserCommitFiles: empty UserCommitFilesRequestHeader")
	}

	if err = validateUserCommitFilesHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "UserCommitFiles: %v", err)
	}

	ctx := stream.Context()
	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.UserCommitFiles(clientCtx)
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

func validateUserCommitFilesHeader(header *pb.UserCommitFilesRequestHeader) error {
	if header.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}
	if len(header.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}
	return nil
}
