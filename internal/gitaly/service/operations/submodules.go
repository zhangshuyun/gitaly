package operations

import (
	"context"
	"fmt"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	if err := validateUserUpdateSubmoduleRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserUpdateSubmodule: %v", err)
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserUpdateSubmodule(clientCtx, req)
}

func validateUserUpdateSubmoduleRequest(req *gitalypb.UserUpdateSubmoduleRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetCommitSha() == "" {
		return fmt.Errorf("empty CommitSha")
	}

	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, req.GetCommitSha()); !match || err != nil {
		return fmt.Errorf("invalid CommitSha")
	}

	if len(req.GetBranch()) == 0 {
		return fmt.Errorf("empty Branch")
	}

	if len(req.GetSubmodule()) == 0 {
		return fmt.Errorf("empty Submodule")
	}

	if len(req.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}

	return nil
}
