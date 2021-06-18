package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) RemoveRepository(ctx context.Context, in *gitalypb.RemoveRepositoryRequest) (*gitalypb.RemoveRepositoryResponse, error) {
	repo := in.GetRepository()

	path, err := s.locator.GetPath(repo)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	storage, ok := s.cfg.Storage(repo.GetStorageName())
	if !ok {
		return nil, helper.ErrInvalidArgumentf("storage %v not found", repo.GetStorageName())
	}

	tempDir := tempdir.TempDir(storage)
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, helper.ErrInternal(err)
	}

	base := filepath.Base(path)
	destDir := filepath.Join(tempDir, base+"+removed")

	if err := s.voteOnAction(ctx, repo, preRemove); err != nil {
		return nil, helper.ErrInternalf("vote on rename: %v", err)
	}

	if err := os.Rename(path, destDir); err != nil {
		// Even in the case where the repository doesn't exist will we cast the same
		// "post-remove" vote as in the case where the repository did exist, but we
		// removed it just fine. The end result is the same in both cases anyway:
		// the repository does not exist.
		if !errors.Is(err, os.ErrNotExist) {
			return nil, helper.ErrInternal(err)
		}
	}

	if err := os.RemoveAll(destDir); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := s.voteOnAction(ctx, repo, postRemove); err != nil {
		return nil, helper.ErrInternalf("vote on finalizing: %v", err)
	}

	return &gitalypb.RemoveRepositoryResponse{}, nil
}

type removalStep int

const (
	preRemove = removalStep(iota)
	postRemove
)

func (s *server) voteOnAction(ctx context.Context, repo *gitalypb.Repository, step removalStep) error {
	return transaction.RunOnContext(ctx, func(tx txinfo.Transaction) error {
		var voteStep string
		switch step {
		case preRemove:
			voteStep = "pre-remove"
		case postRemove:
			voteStep = "post-remove"
		default:
			return fmt.Errorf("invalid removal step: %d", step)
		}

		vote := fmt.Sprintf("%s %s", voteStep, repo.GetRelativePath())
		return s.txManager.Vote(ctx, tx, voting.VoteFromData([]byte(vote)))
	})
}
