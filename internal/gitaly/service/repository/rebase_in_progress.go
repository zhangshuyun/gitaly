package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	worktreePrefix       = "gitlab-worktree"
	rebaseWorktreePrefix = "rebase"
	freshTimeout         = 15 * time.Minute
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

func freshWorktree(ctx context.Context, repoPath, prefix, id string) (bool, error) {
	worktreePath := filepath.Join(repoPath, worktreePrefix, fmt.Sprintf("%s-%s", prefix, id))

	fs, err := os.Stat(worktreePath)
	if err != nil {
		return false, nil
	}

	if time.Since(fs.ModTime()) > freshTimeout {
		if err = os.RemoveAll(worktreePath); err != nil {
			if err = housekeeping.FixDirectoryPermissions(ctx, worktreePath); err != nil {
				return false, err
			}
			err = os.RemoveAll(worktreePath)
		}
		return false, err
	}

	return true, nil
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
