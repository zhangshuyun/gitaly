package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetCommitMessages(request *gitalypb.GetCommitMessagesRequest, stream gitalypb.CommitService_GetCommitMessagesServer) error {
	if err := validateGetCommitMessagesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetCommitMessages: %v", err)
	}

	ctx := stream.Context()

	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetCommitMessages(clientCtx, request)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetCommitMessagesRequest(request *gitalypb.GetCommitMessagesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return nil
}
