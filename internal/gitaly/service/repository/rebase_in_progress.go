package repository

import (
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	rebaseWorktreePrefix = "rebase"
)

func (s *server) IsRebaseInProgress(ctx context.Context, req *gitalypb.IsRebaseInProgressRequest) (*gitalypb.IsRebaseInProgressResponse, error) {
	if err := validateIsRebaseInProgressRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "IsRebaseInProgress: %v", err)
	}

	repoPath, err := s.locator.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	inProg, err := freshWorktree(ctx, repoPath, rebaseWorktreePrefix, req.GetRebaseId())
	if err != nil {
		return nil, err
	}
	return &gitalypb.IsRebaseInProgressResponse{InProgress: inProg}, nil
}

func validateIsRebaseInProgressRequest(req *gitalypb.IsRebaseInProgressRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetRebaseId() == "" {
		return fmt.Errorf("empty RebaseId")
	}

	if strings.Contains(req.GetRebaseId(), "/") {
		return fmt.Errorf("RebaseId contains '/'")
	}

	return nil
}
