package commit

import (
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (*server) FindCommits(req *pb.FindCommitsRequest, stream pb.CommitService_FindCommitsServer) error {
	ctx := stream.Context()

	// Use Gitaly's default branch lookup function because that is already
	// migrated.
	if revision := req.Revision; len(revision) == 0 {
		repoPath, err := helper.GetRepoPath(req.Repository)
		if err != nil {
			return err
		}

		req.Revision, err = defaultBranchName(ctx, repoPath)
		if err != nil {
			return grpc.Errorf(codes.Internal, "defaultBranchName: %v", err)
		}
	}

	client, err := rubyserver.CommitServiceClient(ctx)
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
			return err
		}
		return stream.Send(resp)
	})
}
