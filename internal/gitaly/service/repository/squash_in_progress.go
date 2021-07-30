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
	squashWorktreePrefix = "squash"
	freshTimeout         = 15 * time.Minute
)

func (s *server) IsSquashInProgress(ctx context.Context, req *gitalypb.IsSquashInProgressRequest) (*gitalypb.IsSquashInProgressResponse, error) {
	if err := validateIsSquashInProgressRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "IsSquashInProgress: %v", err)
	}

	repoPath, err := s.locator.GetRepoPath(req.GetRepository())
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
