package repository

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if err := validateWriteRefRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	if err := writeRef(ctx, req); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.WriteRefResponse{}, nil
}

func writeRef(ctx context.Context, req writeRefRequest) error {
	if string(req.GetRef()) == "HEAD" {
		return updateSymbolicRef(ctx, req)
	}
	return updateRef(ctx, req)
}

func updateSymbolicRef(ctx context.Context, req writeRefRequest) error {
	cmd, err := git.SafeCmd(ctx, req.GetRepository(), nil, git.SubCmd{Name: "symbolic-ref", Args: []string{string(req.GetRef()), string(req.GetRevision())}})
	if err != nil {
		return fmt.Errorf("error when creating symbolic-ref command: %v", err)
	}
	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("error when running symbolic-ref command: %v", err)
	}
	return nil
}

func updateRef(ctx context.Context, req writeRefRequest) error {
	u, err := updateref.New(ctx, req.GetRepository())
	if err != nil {
		return fmt.Errorf("error when running creating new updater: %v", err)
	}
	if err = u.Update(string(req.GetRef()), string(req.GetRevision()), string(req.GetOldRevision())); err != nil {
		return fmt.Errorf("error when creating update-ref command: %v", err)
	}
	if err = u.Wait(); err != nil {
		return fmt.Errorf("error when running update-ref command: %v", err)
	}
	return nil
}

func validateWriteRefRequest(req writeRefRequest) error {
	if err := git.ValidateRevision(req.GetRef()); err != nil {
		return fmt.Errorf("invalid ref: %v", err)
	}
	if err := git.ValidateRevision(req.GetRevision()); err != nil {
		return fmt.Errorf("invalid revision: %v", err)
	}
	if len(req.GetOldRevision()) > 0 {
		if err := git.ValidateRevision(req.GetOldRevision()); err != nil {
			return fmt.Errorf("invalid OldRevision: %v", err)
		}
	}

	if !bytes.Equal(req.GetRef(), []byte("HEAD")) && !bytes.HasPrefix(req.GetRef(), []byte("refs/")) {
		return fmt.Errorf("ref has to be a full reference")
	}
	return nil
}
