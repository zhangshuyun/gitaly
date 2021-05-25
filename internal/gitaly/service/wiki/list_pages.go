package wiki

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) WikiListPages(request *gitalypb.WikiListPagesRequest, stream gitalypb.WikiService_WikiListPagesServer) error {
	ctx := stream.Context()

	client, err := s.ruby.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiListPages(clientCtx, request)
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
