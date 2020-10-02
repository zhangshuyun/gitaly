package operations

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserSquash(ctx context.Context, req *gitalypb.UserSquashRequest) (*gitalypb.UserSquashResponse, error) {
	if err := validateUserSquashRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserSquash: %v", err)
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserSquash(clientCtx, req)
}

func validateUserSquashRequest(req *gitalypb.UserSquashRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetSquashId() == "" {
		return fmt.Errorf("empty SquashId")
	}

	if req.GetStartSha() == "" {
		return fmt.Errorf("empty StartSha")
	}

	if req.GetEndSha() == "" {
		return fmt.Errorf("empty EndSha")
	}

	if len(req.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}

	if req.GetAuthor() == nil {
		return fmt.Errorf("empty Author")
	}

	return nil
}
