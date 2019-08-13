package wiki

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WikiFindPage(request *gitalypb.WikiFindPageRequest, stream gitalypb.WikiService_WikiFindPageServer) error {
	ctx := stream.Context()

	if err := validateWikiFindPage(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "WikiFindPage: %v", err)
	}

	client, err := s.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiFindPage(clientCtx, request)
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

func validateWikiFindPage(request *gitalypb.WikiFindPageRequest) error {
	if err := git.ValidateRevisionAllowEmpty(request.Revision); err != nil {
		return err
	}
	if len(request.GetTitle()) == 0 {
		return errors.New("empty Title")
	}
	return nil
}
