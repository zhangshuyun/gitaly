package repository

import (
	"bytes"
	"context"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	diskPath, err := s.locator.GetPath(req.GetRepository())
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("locate repository: %v", err)
	}

	if err := os.MkdirAll(diskPath, 0770); err != nil {
		return nil, helper.ErrInternalf("create directories: %v", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := git.SafeCmdWithoutRepo(ctx, git.CmdStream{Err: stderr}, nil,
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
		return nil, helper.ErrInternalf("create git init: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, helper.ErrInternalf("git init stderr: %q, err: %v", stderr, err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
