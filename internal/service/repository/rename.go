package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) RenameRepository(ctx context.Context, in *gitalypb.RenameRepositoryRequest) (*gitalypb.RenameRepositoryResponse, error) {
	if err := validateRenameRepositoryRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	fromFullPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	toFullPath, err := helper.GetPath(&gitalypb.Repository{StorageName: in.GetRepository().GetStorageName(), RelativePath: in.GetRelativePath()})
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if _, err = os.Stat(toFullPath); !os.IsNotExist(err) {
		return nil, helper.ErrPreconditionFailed(errors.New("destination already exists"))
	}

	if err = os.MkdirAll(filepath.Dir(toFullPath), 0755); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err = os.Rename(fromFullPath, toFullPath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.RenameRepositoryResponse{}, nil
}

func validateRenameRepositoryRequest(in *gitalypb.RenameRepositoryRequest) error {
	if in.GetRepository() == nil {
		return errors.New("from repository is empty")
	}

	if in.GetRelativePath() == "" {
		return errors.New("destination relative path is empty")
	}

	return nil
}
