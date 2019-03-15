package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// PreFetch is unsafe https://gitlab.com/gitlab-org/gitaly/issues/1552
func (s *server) PreFetch(ctx context.Context, req *gitalypb.PreFetchRequest) (*gitalypb.PreFetchResponse, error) {
	return nil, helper.Unimplemented

	/*
		if err := validatePreFetchRequest(req); err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}

		if err := validatePreFetchPrecondition(req); err != nil {
			return nil, helper.ErrPreconditionFailed(err)
		}

		if err := preFetch(ctx, req); err != nil {
			return nil, helper.ErrInternal(err)
		}

		return &gitalypb.PreFetchResponse{}, nil
	*/
}

/*
func validatePreFetchRequest(req *gitalypb.PreFetchRequest) error {
	if req.GetTargetRepository() == nil {
		return errors.New("repository is empty")
	}

	if req.GetSourceRepository() == nil {
		return errors.New("source repository is empty")
	}

	if req.GetSourceRepository().GetStorageName() != req.GetTargetRepository().GetStorageName() {
		return errors.New("source repository and target repository are not on the same storage")
	}

	return nil
}

func validatePreFetchPrecondition(req *gitalypb.PreFetchRequest) error {
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

func preFetch(ctx context.Context, req *gitalypb.PreFetchRequest) error {
	targetRepository, sourceRepository := req.GetTargetRepository(), req.GetSourceRepository()

	sourceRepositoryFullPath, err := helper.GetPath(sourceRepository)
	if err != nil {
		return fmt.Errorf("getting source repository path: %v", err)
	}

	targetRepositoryFullPath, err := helper.GetPath(targetRepository)
	if err != nil {
		return fmt.Errorf("getting target repository path: %v", err)
	}

	targetPath, err := helper.GetPath(targetRepository)
	if err != nil {
		return fmt.Errorf("getting target repository path: %v", err)
	}

	dir := filepath.Dir(targetPath)

	tmpRepoDir, err := ioutil.TempDir(dir, "repo")
	if err != nil {
		return fmt.Errorf("creating temp directory for repo: %v", err)
	}
	defer os.RemoveAll(tmpRepoDir)

	storagePath, err := helper.GetStorageByName(targetRepository.GetStorageName())
	if err != nil {
		return fmt.Errorf("getting storage path for target repo: %v", err)
	}

	relativePath, err := filepath.Rel(storagePath, tmpRepoDir)
	if err != nil {
		return fmt.Errorf("getting relative path for temp repo: %v", err)
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
		return fmt.Errorf("clone command: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("clone command: %v", err)
	}

	objectPool, err := objectpool.FromProto(req.GetObjectPool())
	if err != nil {
		return fmt.Errorf("getting object pool: %v", err)
	}

	// As of 11.9, Link will still create remotes in the object pool. In this case the remotes will point to the tempoarary
	// directory. This is OK because we don't plan on using these remotes, and will remove them in the future.
	if err := objectPool.Link(ctx, tmpRepo); err != nil {
		return fmt.Errorf("linking: %v", err)
	}

	return os.Rename(tmpRepoDir, targetRepositoryFullPath)
}
*/
