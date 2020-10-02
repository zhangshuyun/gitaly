package operations

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCommitFiles(stream gitalypb.OperationService_UserCommitFilesServer) error {
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
	client, err := s.ruby.OperationServiceClient(ctx)
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

func validateUserCommitFilesHeader(header *gitalypb.UserCommitFilesRequestHeader) error {
	if header.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}
	if len(header.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}
	if len(header.GetBranchName()) == 0 {
		return fmt.Errorf("empty BranchName")
	}

	startSha := header.GetStartSha()
	if len(startSha) > 0 {
		err := git.ValidateCommitID(startSha)
		if err != nil {
			return err
		}
	}

	return nil
}
