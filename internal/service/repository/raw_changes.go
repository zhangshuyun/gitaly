package repository

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetRawChanges(req *gitalypb.GetRawChangesRequest, stream gitalypb.RepositoryService_GetRawChangesServer) error {
	if req.GetRepository() == nil {
		return status.Errorf(codes.InvalidArgument, "repository argument must be present")
	}

	if req.GetFromRevision() == "" || req.GetToRevision() == "" {
		return status.Errorf(codes.InvalidArgument, "from and to revision must be present")
	}

	ctx := stream.Context()

	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetRawChanges(clientCtx, req)
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
