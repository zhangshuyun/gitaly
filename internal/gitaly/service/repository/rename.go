package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) RenameRepository(ctx context.Context, in *gitalypb.RenameRepositoryRequest) (*gitalypb.RenameRepositoryResponse, error) {
	if err := validateRenameRepositoryRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if featureflag.RenameRepositoryLocking.IsEnabled(ctx) {
		targetRepo := &gitalypb.Repository{
			StorageName:  in.GetRepository().GetStorageName(),
			RelativePath: in.GetRelativePath(),
		}

		if err := s.renameRepository(ctx, in.GetRepository(), targetRepo); err != nil {
			return nil, helper.ErrInternal(err)
		}

		return &gitalypb.RenameRepositoryResponse{}, nil
	}

	fromFullPath, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	toFullPath, err := s.locator.GetPath(&gitalypb.Repository{StorageName: in.GetRepository().GetStorageName(), RelativePath: in.GetRelativePath()})
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if _, err = os.Stat(toFullPath); !os.IsNotExist(err) {
		return nil, helper.ErrFailedPreconditionf("destination already exists")
	}

	if err = os.MkdirAll(filepath.Dir(toFullPath), 0o755); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err = os.Rename(fromFullPath, toFullPath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.RenameRepositoryResponse{}, nil
}

func (s *server) renameRepository(ctx context.Context, sourceRepo, targetRepo *gitalypb.Repository) error {
	sourcePath, err := s.locator.GetRepoPath(sourceRepo)
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	targetPath, err := s.locator.GetPath(targetRepo)
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	// Check up front whether the target path exists already. If it does, we can avoid going
	// into the critical section altogether.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return helper.ErrAlreadyExistsf("target repo exists already")
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o770); err != nil {
		return fmt.Errorf("create target parent dir: %w", err)
	}

	// We're locking both the source repository path and the target repository path for
	// concurrent modification. This is so that the source repo doesn't get moved somewhere else
	// meanwhile, and so that the target repo doesn't get created concurrently either.
	sourceLocker, err := safe.NewLockingFileWriter(sourcePath)
	if err != nil {
		return fmt.Errorf("creating source repo locker: %w", err)
	}
	defer func() {
		if err := sourceLocker.Close(); err != nil {
			ctxlogrus.Extract(ctx).Error("closing source repo locker: %w", err)
		}
	}()

	targetLocker, err := safe.NewLockingFileWriter(targetPath)
	if err != nil {
		return fmt.Errorf("creating target repo locker: %w", err)
	}
	defer func() {
		if err := targetLocker.Close(); err != nil {
			ctxlogrus.Extract(ctx).Error("closing target repo locker: %w", err)
		}
	}()

	// We're now entering the critical section where both the source and target path are locked.
	if err := sourceLocker.Lock(); err != nil {
		return fmt.Errorf("locking source repo: %w", err)
	}
	if err := targetLocker.Lock(); err != nil {
		return fmt.Errorf("locking target repo: %w", err)
	}

	// We need to re-check whether the target path exists in case somebody has removed it before
	// we have taken the lock.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return helper.ErrAlreadyExistsf("target repo exists already")
	}

	if err := os.Rename(sourcePath, targetPath); err != nil {
		return fmt.Errorf("moving repository into place: %w", err)
	}

	return nil
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
