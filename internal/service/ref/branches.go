package ref

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
)

func (s *server) CreateBranch(ctx context.Context, req *gitalypb.CreateBranchRequest) (*gitalypb.CreateBranchResponse, error) {
	client, err := s.RefServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.CreateBranch(clientCtx, req)
}

func (s *server) DeleteBranch(ctx context.Context, req *gitalypb.DeleteBranchRequest) (*gitalypb.DeleteBranchResponse, error) {
	client, err := s.RefServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.DeleteBranch(clientCtx, req)
}

func (s *server) FindBranch(ctx context.Context, req *gitalypb.FindBranchRequest) (*gitalypb.FindBranchResponse, error) {
	client, err := s.RefServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FindBranch(clientCtx, req)
}
