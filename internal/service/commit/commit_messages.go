package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetCommitMessages(request *gitalypb.GetCommitMessagesRequest, stream gitalypb.CommitService_GetCommitMessagesServer) error {
	if err := validateGetCommitMessagesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetCommitMessages: %v", err)
	}
	ctx := stream.Context()
	for _, commitID := range request.GetCommitIds() {
		msg, err := log.GetCommitMessage(ctx, request.GetRepository(), commitID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get commit message: %v", err)
		}
		stream.Send(&gitalypb.GetCommitMessagesResponse{CommitId: commitID, Message: msg})
	}
	return nil
}

func validateGetCommitMessagesRequest(request *gitalypb.GetCommitMessagesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return nil
}
