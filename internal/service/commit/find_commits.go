package commit

import (
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FindCommits(req *pb.FindCommitsRequest, stream pb.CommitService_FindCommitsServer) error {
	ctx := stream.Context()

	// Use Gitaly's default branch lookup function because that is already
	// migrated.
	if revision := req.Revision; len(revision) == 0 {
		var err error
		req.Revision, err = defaultBranchName(ctx, req.Repository)
		if err != nil {
			return status.Errorf(codes.Internal, "defaultBranchName: %v", err)
		}
	}

	// Clients might send empty paths. That is an error
	for _, path := range req.Paths {
		if len(path) == 0 {
			return status.Errorf(codes.InvalidArgument, "path is empty string")
		}
	}

	// We are removing the 'rugged walk' implementation
	req.DisableWalk = true

	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.FindCommits(clientCtx, req)
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
