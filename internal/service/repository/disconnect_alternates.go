package repository

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) DisconnectAlternates(ctx context.Context, req *gitalypb.DisconnectAlternatesRequest) (*gitalypb.DisconnectAlternatesResponse, error) {
	if req.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(errors.New("empty repository"))
	}

	if err := disconnectAlternates(req.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.DisconnectAlternatesResponse{}, nil
}

func disconnectAlternates(repository repository.GitRepo) error {
	altPath, err := git.AlternatesPath(repository)
	if err != nil {
		return err
	}

	_, err = os.Stat(altPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("error when reading alternates file: %v", err)
	}

	return os.Remove(altPath)
}
