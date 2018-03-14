package repository

import (
	"io"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

var hlbRuby = false

// Deprecated
func (s *server) Exists(ctx context.Context, in *pb.RepositoryExistsRequest) (*pb.RepositoryExistsResponse, error) {
	return nil, helper.Unimplemented
}

func (s *server) RepositoryExists(ctx context.Context, in *pb.RepositoryExistsRequest) (*pb.RepositoryExistsResponse, error) {
	path, err := helper.GetPath(in.Repository)
	if err != nil {
		return nil, err
	}

	return &pb.RepositoryExistsResponse{Exists: helper.IsGitDirectory(path)}, nil
}

func rubyHasLocalBranches(s *server, ctx context.Context, in *pb.HasLocalBranchesRequest) (*pb.HasLocalBranchesResponse, error) {
	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.HasLocalBranches(clientCtx, in)
}

func (s *server) HasLocalBranches(ctx context.Context, in *pb.HasLocalBranchesRequest) (*pb.HasLocalBranchesResponse, error) {
	if hlbRuby {
		return rubyHasLocalBranches(s, ctx, in)
	}

	args := []string{"for-each-ref", "--count=1", "refs/heads"}
	cmd, err := git.Command(ctx, in.GetRepository(), args...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: gitCommand: %v", err)
	}

	buf := make([]byte, 1)
	n, err := cmd.Read(buf)
	if err != nil && err != io.EOF {
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: read: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: cmd wait: %v", err)
	}

	return &pb.HasLocalBranchesResponse{Value: n > 0}, nil
}
