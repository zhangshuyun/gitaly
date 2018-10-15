package operations

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCreateBranch(clientCtx, req)
}

func (s *server) UserUpdateBranch(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserUpdateBranch(clientCtx, req)
}

func (s *server) UserDeleteBranch(ctx context.Context, req *gitalypb.UserDeleteBranchRequest) (*gitalypb.UserDeleteBranchResponse, error) {
	if len(req.BranchName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty branch name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	client, err := s.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteBranch(clientCtx, req)
}
