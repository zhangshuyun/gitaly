package ref

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) PackRefs(ctx context.Context, in *gitalypb.PackRefsRequest) (*gitalypb.PackRefsResponse, error) {
	if err := validatePackRefsRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := s.packRefs(ctx, in.GetRepository(), in.GetAllRefs()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.PackRefsResponse{}, nil
}

func validatePackRefsRequest(in *gitalypb.PackRefsRequest) error {
	if in.GetRepository() == nil {
		return errors.New("empty repository")
	}
	return nil
}

func (s *server) packRefs(ctx context.Context, repository repository.GitRepo, all bool) error {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{
		Name:  "pack-refs",
		Flags: []git.Option{git.Flag{Name: "--all"}},
	})
	if err != nil {
		return fmt.Errorf("initializing pack-refs command: %v", err)
	}

	return cmd.Wait()
}
