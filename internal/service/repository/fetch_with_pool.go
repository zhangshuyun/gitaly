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
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

const (
	//GeoRemoteName is the temporary remote name we use for fetching
	GeoRemoteName = "geo"
)

func (s *server) GeoFetchWithPool(ctx context.Context, req *gitalypb.GeoFetchWithPoolRequest) (*gitalypb.GeoFetchWithPoolResponse, error) {
	if err := validateGeoFetchWithPoolRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := validateGeoFetchWithPoolPrecondition(req); err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	// pre fetch from the fork source
	tmpRepo, err := preFetch(ctx, req)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	_, err = s.SetConfig(ctx, &gitalypb.SetConfigRequest{
		Repository: tmpRepo,
		Entries:    []*gitalypb.SetConfigRequest_Entry{req.GetJwtAuthenticationHeader()},
	})
	if err != nil {
		return nil, helper.ErrInternal(err)
	}
	defer func() {
		_, err = s.DeleteConfig(ctx, &gitalypb.DeleteConfigRequest{
			Repository: req.GetTargetRepository(),
			Keys:       []string{req.GetJwtAuthenticationHeader().GetKey()},
		})
	}()

	tmpRepoDir, err := helper.GetPath(tmpRepo)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}
	defer os.RemoveAll(tmpRepoDir)

	// add the remote
	if err := s.addRemote(ctx, tmpRepo, GeoRemoteName, req.GetRemoteUrl()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	// fetch
	if err := s.fetchRemote(ctx, tmpRepo, GeoRemoteName); err != nil {
		return nil, helper.ErrInternal(err)
	}

	// rename
	targetRepositoryFullPath, err := helper.GetPath(req.GetTargetRepository())
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := os.Rename(tmpRepoDir, targetRepositoryFullPath); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.GeoFetchWithPoolResponse{}, nil
}

func validateGeoFetchWithPoolRequest(req *gitalypb.GeoFetchWithPoolRequest) error {
	if req.GetTargetRepository() == nil {
		return errors.New("repository is empty")
	}

	if req.GetSourceRepository() == nil {
		return errors.New("source repository is empty")
	}

	if req.GetSourceRepository().GetStorageName() != req.GetTargetRepository().GetStorageName() {
		return errors.New("source repository and target repository are not on the same storage")
	}

	if req.GetRemoteUrl() == "" {
		return errors.New("missing remote url")
	}

	return nil
}

func validateGeoFetchWithPoolPrecondition(req *gitalypb.GeoFetchWithPoolRequest) error {
	targetRepositoryFullPath, err := helper.GetPath(req.GetTargetRepository())
	if err != nil {
		return fmt.Errorf("getting target repository path: %v", err)
	}

	if _, err := os.Stat(targetRepositoryFullPath); !os.IsNotExist(err) {
		return errors.New("target reopsitory already exists")
	}

	objectPool, err := objectpool.FromProto(req.GetObjectPool())
	if err != nil {
		return fmt.Errorf("getting object pool from repository: %v", err)
	}

	if !objectPool.Exists() {
		return errors.New("object pool does not exist")
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

func preFetch(ctx context.Context, req *gitalypb.GeoFetchWithPoolRequest) (*gitalypb.Repository, error) {
	targetRepository, sourceRepository := req.GetTargetRepository(), req.GetSourceRepository()

	sourceRepositoryFullPath, err := helper.GetPath(sourceRepository)
	if err != nil {
		return nil, fmt.Errorf("getting source repository path: %v", err)
	}

	targetPath, err := helper.GetPath(targetRepository)
	if err != nil {
		return nil, fmt.Errorf("getting target repository path: %v", err)
	}

	dir := filepath.Dir(targetPath)

	tmpRepoDir, err := ioutil.TempDir(dir, "repo")
	if err != nil {
		return nil, fmt.Errorf("creating temp directory for repo: %v", err)
	}

	storagePath, err := helper.GetStorageByName(targetRepository.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("getting storage path for target repo: %v", err)
	}

	relativePath, err := filepath.Rel(storagePath, tmpRepoDir)
	if err != nil {
		return nil, fmt.Errorf("getting relative path for temp repo: %v", err)
	}

	tmpRepo := &gitalypb.Repository{
		RelativePath: relativePath,
		StorageName:  targetRepository.GetStorageName(),
	}

	args := []string{
		"clone",
		"--bare",
		"--shared",
		"--",
		sourceRepositoryFullPath,
		tmpRepoDir,
	}

	cmd, err := git.BareCommand(ctx, nil, nil, nil, nil, args...)
	if err != nil {
		return nil, fmt.Errorf("clone command: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("clone command: %v", err)
	}

	objectPool, err := objectpool.FromProto(req.GetObjectPool())
	if err != nil {
		return nil, fmt.Errorf("getting object pool: %v", err)
	}

	// As of 11.9, Link will still create remotes in the object pool. In this case the remotes will point to the tempoarary
	// directory. This is OK because we don't plan on using these remotes, and will remove them in the future.
	if err := objectPool.Link(ctx, tmpRepo); err != nil {
		return nil, fmt.Errorf("linking: %v", err)
	}

	return tmpRepo, nil
}

func (s *server) addRemote(ctx context.Context, repository *gitalypb.Repository, name, url string) error {
	client, err := s.RemoteServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, repository)
	if err != nil {
		return err
	}

	if _, err = client.AddRemote(clientCtx, &gitalypb.AddRemoteRequest{
		Repository:    repository,
		Name:          name,
		Url:           url, //need to set url here
		MirrorRefmaps: []string{"all_refs"},
	}); err != nil {
		return err
	}
	return nil
}

func (s *server) fetchRemote(ctx context.Context, repository *gitalypb.Repository, remoteName string) error {
	// fetch the remote
	fetchRemoteRequest := &gitalypb.FetchRemoteRequest{
		Repository: repository,
		Remote:     remoteName,
		Force:      false,
		Timeout:    1000,
	}

	_, err := s.FetchRemote(ctx, fetchRemoteRequest)
	return err
}
