package repository

import (
	"context"
	"os"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	diskPath, err := s.locator.GetPath(req.GetRepository())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := os.MkdirAll(diskPath, 0770); err != nil {
		return nil, helper.ErrInternal(err)
	}

	cmd, err := git.SafeCmdWithoutRepo(ctx, git.CmdStream{}, nil,
		git.SubCmd{
			Name: "init",
			Flags: []git.Option{
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--quiet"},
			},
			Args: []string{diskPath},
		},
	)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
