package diff

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CommitPatch(in *gitalypb.CommitPatchRequest, stream gitalypb.DiffService_CommitPatchServer) error {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()

	client, err := s.DiffServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.CommitPatch(clientCtx, in)
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
