package repository

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
		return s.updateSymbolicRef(ctx, repo, req)
	}
	return updateRef(ctx, s.cfg, repo, req)
}

func (s *server) updateSymbolicRef(ctx context.Context, repo *localrepo.Repo, req *gitalypb.WriteRefRequest) error {
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "symbolic-ref",
			Args: []string{string(req.GetRef()), string(req.GetRevision())},
		},
		git.WithRefTxHook(ctx, req.GetRepository(), s.cfg),
	); err != nil {
		return fmt.Errorf("error when running symbolic-ref command: %v", err)
	}
	return nil
}

func updateRef(ctx context.Context, cfg config.Cfg, repo *localrepo.Repo, req *gitalypb.WriteRefRequest) error {
	u, err := updateref.New(ctx, cfg, repo)
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %v", err)
	}
	if err = u.Update(git.ReferenceName(req.GetRef()), string(req.GetRevision()), string(req.GetOldRevision())); err != nil {
		return fmt.Errorf("error when creating update-ref command: %v", err)
	}
	if err = u.Wait(); err != nil {
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
