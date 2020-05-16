package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CreateFullBackup(ctx context.Context, in *gitalypb.CreateFullBackupRequest) (*gitalypb.CreateFullBackupResponse, error) {

	return &gitalypb.CreateFullBackupResponse{}, nil
}
