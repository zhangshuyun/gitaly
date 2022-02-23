package ref

import (
	"context"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) PackRefs(ctx context.Context, in *gitalypb.PackRefsRequest) (*gitalypb.PackRefsResponse, error) {
	if err := validatePackRefsRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := s.packRefs(ctx, in.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.PackRefsResponse{}, nil
}

func validatePackRefsRequest(in *gitalypb.PackRefsRequest) error {
	if in.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}
	return nil
}

func (s *server) packRefs(ctx context.Context, repository repository.GitRepo) error {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name:  "pack-refs",
		Flags: []git.Option{git.Flag{Name: "--all"}},
	})
	if err != nil {
		return err
	}

	return cmd.Wait()
}
