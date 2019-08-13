package wiki

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WikiFindFile(request *gitalypb.WikiFindFileRequest, stream gitalypb.WikiService_WikiFindFileServer) error {
	ctx := stream.Context()

	if err := git.ValidateRevisionAllowEmpty(request.Revision); err != nil {
		return status.Errorf(codes.InvalidArgument, "WikiFindFile: %s", err)
	}

	if len(request.GetName()) == 0 {
		return status.Errorf(codes.InvalidArgument, "WikiFindFile: Empty Name")
	}

	client, err := s.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiFindFile(clientCtx, request)
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
