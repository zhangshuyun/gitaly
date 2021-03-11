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
		return nil, helper.ErrInvalidArgumentf("locate repository: %w", err)
	}

	if err := os.MkdirAll(diskPath, 0770); err != nil {
		return nil, helper.ErrInternalf("create directories: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "init",
			Flags: []git.Option{
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--quiet"},
			},
			Args: []string{diskPath},
		},
		git.WithStderr(stderr),
	)
	if err != nil {
		return nil, helper.ErrInternalf("create git init: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, helper.ErrInternalf("git init stderr: %q, err: %w", stderr, err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
