package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetCommitSignatures(request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	if err := validateGetCommitSignaturesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetCommitSignatures: %v", err)
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

	rubyStream, err := client.GetCommitSignatures(clientCtx, request)
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

func validateGetCommitSignaturesRequest(request *gitalypb.GetCommitSignaturesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if len(request.GetCommitIds()) == 0 {
		return fmt.Errorf("empty CommitIds")
	}

	return nil
}
