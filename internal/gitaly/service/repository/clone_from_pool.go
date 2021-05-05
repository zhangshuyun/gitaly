package repository

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CloneFromPool(ctx context.Context, req *gitalypb.CloneFromPoolRequest) (*gitalypb.CloneFromPoolResponse, error) {
	if err := validateCloneFromPoolRequestArgs(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := s.validateCloneFromPoolRequestRepositoryState(req); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := s.cloneFromPool(ctx, req.GetPool(), req.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if _, err := s.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository:   req.GetRepository(),
		RemoteParams: req.GetRemote(),
		Timeout:      1000,
	}); err != nil {
		return nil, helper.ErrInternalf("fetch http remote: %v", err)
	}

	objectPool, err := objectpool.FromProto(s.cfg, s.locator, s.gitCmdFactory, s.catfileCache, req.GetPool())
	if err != nil {
		return nil, helper.ErrInternalf("get object pool from request: %v", err)
	}

	if err = objectPool.Link(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("change hard link to relative: %v", err)
	}

	return &gitalypb.CloneFromPoolResponse{}, nil
}

func (s *server) validateCloneFromPoolRequestRepositoryState(req *gitalypb.CloneFromPoolRequest) error {
	targetRepositoryFullPath, err := s.locator.GetPath(req.GetRepository())
	if err != nil {
		return fmt.Errorf("getting target repository path: %v", err)
	}

	if _, err := os.Stat(targetRepositoryFullPath); !os.IsNotExist(err) {
		return errors.New("target reopsitory already exists")
	}

	objectPool, err := objectpool.FromProto(s.cfg, s.locator, s.gitCmdFactory, s.catfileCache, req.GetPool())
	if err != nil {
		return fmt.Errorf("getting object pool from repository: %v", err)
	}

	if !objectPool.IsValid() {
		return errors.New("object pool is not valid")
	}

	return nil
}

func validateCloneFromPoolRequestArgs(req *gitalypb.CloneFromPoolRequest) error {
	if req.GetRepository() == nil {
		return errors.New("repository required")
	}

	if req.GetRemote() == nil {
		return errors.New("remote required")
	}

	if req.GetPool() == nil {
		return errors.New("pool is empty")
	}

	return nil
}
