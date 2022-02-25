package repository

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if err := validateWriteRefRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	if err := s.writeRef(ctx, req); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.WriteRefResponse{}, nil
}

func (s *server) writeRef(ctx context.Context, req *gitalypb.WriteRefRequest) error {
	repo := s.localrepo(req.GetRepository())

	if string(req.Ref) == "HEAD" {
		if err := repo.SetDefaultBranch(ctx, s.txManager, git.ReferenceName(req.GetRevision())); err != nil {
			return fmt.Errorf("setting default branch: %v", err)
		}

		return nil
	}

	return updateRef(ctx, repo, req)
}

func updateRef(ctx context.Context, repo *localrepo.Repo, req *gitalypb.WriteRefRequest) error {
	u, err := updateref.New(ctx, repo)
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %v", err)
	}
	if err = u.Update(git.ReferenceName(req.GetRef()), string(req.GetRevision()), string(req.GetOldRevision())); err != nil {
		return fmt.Errorf("error when creating update-ref command: %v", err)
	}
	if err = u.Commit(); err != nil {
		return fmt.Errorf("error when running update-ref command: %v", err)
	}
	return nil
}

func validateWriteRefRequest(req *gitalypb.WriteRefRequest) error {
	if err := git.ValidateRevision(req.Ref); err != nil {
		return fmt.Errorf("invalid ref: %v", err)
	}
	if err := git.ValidateRevision(req.Revision); err != nil {
		return fmt.Errorf("invalid revision: %v", err)
	}
	if len(req.OldRevision) > 0 {
		if err := git.ValidateRevision(req.OldRevision); err != nil {
			return fmt.Errorf("invalid OldRevision: %v", err)
		}
	}

	if !bytes.Equal(req.Ref, []byte("HEAD")) && !bytes.HasPrefix(req.Ref, []byte("refs/")) {
		return fmt.Errorf("ref has to be a full reference")
	}
	return nil
}
