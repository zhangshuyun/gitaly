package conflicts

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ListConflictFiles(in *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if err := validateListConflictFilesRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "ListConflictFiles: %v", err)
	}

	client, err := s.ruby.ConflictsServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.ListConflictFiles(clientCtx, in)
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

func validateListConflictFilesRequest(in *gitalypb.ListConflictFilesRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if in.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if in.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}

	return nil
}
