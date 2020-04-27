package repository

import (
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	squashWorktreePrefix = "squash"
)

func (s *server) IsSquashInProgress(ctx context.Context, req *gitalypb.IsSquashInProgressRequest) (*gitalypb.IsSquashInProgressResponse, error) {
	if err := validateIsSquashInProgressRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "IsSquashInProgress: %v", err)
	}

	repoPath, err := helper.GetValidatedRepoPath(req.GetRepository(), s.storages)
	if err != nil {
		return nil, err
	}

	inProg, err := freshWorktree(ctx, repoPath, squashWorktreePrefix, req.GetSquashId())
	if err != nil {
		return nil, err
	}
	return &gitalypb.IsSquashInProgressResponse{InProgress: inProg}, nil
}

func validateIsSquashInProgressRequest(req *gitalypb.IsSquashInProgressRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetSquashId() == "" {
		return fmt.Errorf("empty SquashId")
	}

	if strings.Contains(req.GetSquashId(), "/") {
		return fmt.Errorf("SquashId contains '/'")
	}

	return nil
}
