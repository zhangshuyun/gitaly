package repository

import (
	"context"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	diskPath, err := helper.GetRepositoryPath(req.GetRepository(), s.storages)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := os.MkdirAll(diskPath, 0770); err != nil {
		return nil, helper.ErrInternal(err)
	}

	cmd, err := git.SafeCmdWithoutRepo(ctx, nil,
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
