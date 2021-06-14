package repository

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CloneFromPoolInternal(ctx context.Context, req *gitalypb.CloneFromPoolInternalRequest) (*gitalypb.CloneFromPoolInternalResponse, error) {
	if err := validateCloneFromPoolInternalRequestArgs(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := s.validateCloneFromPoolInternalRequestRepositoryState(req); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := s.cloneFromPool(ctx, req.GetPool(), req.GetRepository()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	repo := s.localrepo(req.GetRepository())

	if err := remote.FetchInternalRemote(ctx, s.cfg, s.conns, repo, req.GetSourceRepository()); err != nil {
		return nil, helper.ErrInternalf("fetch internal remote: %v", err)
	}

	objectPool, err := objectpool.FromProto(s.cfg, s.locator, s.gitCmdFactory, s.catfileCache, req.GetPool())
	if err != nil {
		return nil, helper.ErrInternalf("get object pool from request: %v", err)
	}

	if err = objectPool.Link(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("change hard link to relative: %v", err)
	}

	return &gitalypb.CloneFromPoolInternalResponse{}, nil
}

func (s *server) validateCloneFromPoolInternalRequestRepositoryState(req *gitalypb.CloneFromPoolInternalRequest) error {
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

	linked, err := objectPool.LinkedToRepository(req.GetSourceRepository())
	if err != nil {
		return fmt.Errorf("error when testing if source repository is linked to pool repository: %v", err)
	}

	if !linked {
		return errors.New("source repository is not linked to pool repository")
	}

	return nil
}

func validateCloneFromPoolInternalRequestArgs(req *gitalypb.CloneFromPoolInternalRequest) error {
	if req.GetRepository() == nil {
		return errors.New("repository required")
	}

	if req.GetSourceRepository() == nil {
		return errors.New("source repository required")
	}

	if req.GetPool() == nil {
		return errors.New("pool is empty")
	}

	if req.GetSourceRepository().GetStorageName() != req.GetRepository().GetStorageName() {
		return errors.New("source repository and target repository are not on the same storage")
	}

	return nil
}

func (s *server) cloneFromPool(ctx context.Context, objectPoolRepo *gitalypb.ObjectPool, repo repository.GitRepo) error {
	objectPoolPath, err := s.locator.GetPath(objectPoolRepo.GetRepository())
	if err != nil {
		return fmt.Errorf("could not get object pool path: %v", err)
	}
	repositoryPath, err := s.locator.GetPath(repo)
	if err != nil {
		return fmt.Errorf("could not get object pool path: %v", err)
	}

	pbRepo, ok := repo.(*gitalypb.Repository)
	if !ok {
		return fmt.Errorf("expected *gitlaypb.Repository but got %T", repo)
	}

	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name:        "clone",
			Flags:       []git.Option{git.Flag{Name: "--bare"}, git.Flag{Name: "--shared"}},
			PostSepArgs: []string{objectPoolPath, repositoryPath},
		},
		git.WithRefTxHook(ctx, pbRepo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("clone with object pool start: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("clone with object pool wait %v", err)
	}

	return nil
}
