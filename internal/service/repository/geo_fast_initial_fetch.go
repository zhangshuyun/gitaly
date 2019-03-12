package repository

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) GeoFastInitialFetch(ctx context.Context, req *gitalypb.GeoFastInitialFetchRequest) (*gitalypb.GeoFastInitialFetchResponse, error) {
	if err := validateGeoFastInitialFetchRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := validateGeoFastInitialFetchPrecondition(req); err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	tmpRepo, err := preFetch(ctx, req)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if _, err := s.FetchHTTPRemote(ctx, &gitalypb.FetchHTTPRemoteRequest{
		Repository: tmpRepo,
		Remote:     req.GetRemote(),
		Timeout:    req.GetTimeout(),
	}); err != nil {
		return nil, helper.ErrInternal(err)
	}

	tmpRepoDir, err := helper.GetPath(tmpRepo)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}
	defer os.RemoveAll(tmpRepoDir)

	// rename
	repositoryFullPath, err := helper.GetPath(req.GetRepository())
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := os.Rename(tmpRepoDir, repositoryFullPath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.GeoFastInitialFetchResponse{}, nil
}

func validateGeoFastInitialFetchRequest(req *gitalypb.GeoFastInitialFetchRequest) error {
	if req.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	if req.GetObjectPool() == nil {
		return errors.New("object pool is empty")
	}

	return nil
}

func validateGeoFastInitialFetchPrecondition(req *gitalypb.GeoFastInitialFetchRequest) error {
	repositoryFullPath, err := helper.GetPath(req.GetRepository())
	if err != nil {
		return fmt.Errorf("getting target repository path: %v", err)
	}

	if _, err := os.Stat(repositoryFullPath); !os.IsNotExist(err) {
		return errors.New("target reopsitory already exists")
	}

	poolRepo := req.GetObjectPool().GetRepository()
	objectPool, err := objectpool.NewObjectPool(poolRepo.GetStorageName(), poolRepo.GetRelativePath())
	if err != nil {
		return fmt.Errorf("getting object pool from repository: %v", err)
	}

	if !objectPool.Exists() {
		return errors.New("object pool does not exist")
	}

	if !objectPool.IsValid() {
		return errors.New("object pool is not valid")
	}

	return nil
}

func preFetch(ctx context.Context, req *gitalypb.GeoFastInitialFetchRequest) (*gitalypb.Repository, error) {
	repository := req.GetRepository()

	repositoryPath, err := helper.GetPath(repository)
	if err != nil {
		return nil, fmt.Errorf("getting repository path: %v", err)
	}

	objectPoolPath, err := helper.GetPath(req.GetObjectPool().GetRepository())
	if err != nil {
		return nil, fmt.Errorf("getting object pool path: %v", err)
	}

	dir := filepath.Dir(repositoryPath)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0770); err != nil {
			return nil, err
		}
	}

	tmpRepoDir, err := ioutil.TempDir(dir, "repo")
	if err != nil {
		return nil, fmt.Errorf("creating temp directory for repo: %v", err)
	}

	storagePath, err := helper.GetStorageByName(repository.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("getting storage path for target repo: %v", err)
	}

	relativePath, err := filepath.Rel(storagePath, tmpRepoDir)
	if err != nil {
		return nil, fmt.Errorf("getting relative path for temp repo: %v", err)
	}

	tmpRepo := &gitalypb.Repository{
		RelativePath: relativePath,
		StorageName:  repository.GetStorageName(),
	}

	args := []string{
		"clone",
		"--bare",
		"--shared",
		"--",
		objectPoolPath,
		tmpRepoDir,
	}

	cmd, err := git.BareCommand(ctx, nil, nil, nil, nil, args...)
	if err != nil {
		return nil, fmt.Errorf("clone command: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("clone command: %v", err)
	}

	poolRepo := req.GetObjectPool().GetRepository()
	objectPool, err := objectpool.NewObjectPool(poolRepo.GetStorageName(), poolRepo.GetRelativePath())
	if err != nil {
		return nil, fmt.Errorf("getting object pool from repository: %v", err)
	}

	if err := objectPool.Link(ctx, tmpRepo); err != nil {
		return nil, fmt.Errorf("linking object pool to temp repo: %v", err)
	}

	return tmpRepo, nil
}
