package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetCommitMessages(request *gitalypb.GetCommitMessagesRequest, stream gitalypb.CommitService_GetCommitMessagesServer) error {
	if err := validateGetCommitMessagesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetCommitMessages: %v", err)
	}
	ctx := stream.Context()
	for _, commitID := range request.GetCommitIds() {
		resp := &gitalypb.GetCommitMessagesResponse{
			CommitId: commitID,
		}

		msgReader, err := log.GetCommitMessage(ctx, request.GetRepository(), commitID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get commit message: %v", err)
		}

		chunk := make([]byte, helper.MaxCommitOrTagMessageSize)
		for n, err := msgReader.Read(chunk); err == nil; {
			resp.Message = chunk[:n]
			stream.Send(resp)

			// read the next chunk
			n, err = msgReader.Read(chunk)
			// reset the response
			resp = &gitalypb.GetCommitMessagesResponse{}
		}
	}
	return nil
}

func validateGetCommitMessagesRequest(request *gitalypb.GetCommitMessagesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return nil
}
