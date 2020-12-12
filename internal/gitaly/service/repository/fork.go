package repository

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	targetRepository := req.Repository
	sourceRepository := req.SourceRepository

	if err := validateCreateForkRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	targetRepositoryFullPath, err := s.locator.GetPath(targetRepository)
	if err != nil {
		return nil, err
	}

	if info, err := os.Stat(targetRepositoryFullPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, status.Errorf(codes.Internal, "CreateFork: check destination path: %v", err)
		}

		// directory does not exist, proceed
	} else {
		if !info.IsDir() {
			return nil, status.Errorf(codes.InvalidArgument, "CreateFork: destination path exists")
		}

		if err := os.Remove(targetRepositoryFullPath); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "CreateFork: destination directory is not empty")
		}
	}

	if err := os.MkdirAll(targetRepositoryFullPath, 0770); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: create dest dir: %v", err)
	}

	objectPool, sourceObjectPoolPath, err := s.getObjectPool(req)
	if err != nil {
		return nil, err
	}

	flags := []git.Option{
		git.Flag{Name: "--bare"},
	}
	var args []string
	var postSepArgs []string

	if req.GetAllowLocal() && sourceRepository.GetStorageName() == targetRepository.GetStorageName() {
		sourceRepositoryFullPath, err := s.locator.GetPath(sourceRepository)
		if err != nil {
			return nil, err
		}

		if _, err := os.Stat(sourceRepositoryFullPath); err != nil {
			if !os.IsNotExist(err) {
				return nil, status.Errorf(codes.Internal, "CreateFork: check source path: %v", err)
			}
		}

		flags = append(flags, git.Flag{Name: "--local"}, git.Flag{Name: "--no-hardlinks"})
		args = []string{sourceRepositoryFullPath}
		postSepArgs = []string{targetRepositoryFullPath}
	} else {
		flags = append(flags, git.Flag{Name: "--no-local"})
		postSepArgs = []string{
			fmt.Sprintf("%s:%s", gitalyssh.GitalyInternalURL, sourceRepository.RelativePath),
			targetRepositoryFullPath,
		}
	}

	if objectPool != nil {
		flags = append(flags, git.ValueFlag{Name: "--reference", Value: sourceObjectPoolPath})
		ctxlogrus.AddFields(ctx, logrus.Fields{"object_pool_path": sourceObjectPoolPath})
	}

	ctxlogrus.AddFields(ctx, logrus.Fields{
		"source_storage": sourceRepository.GetStorageName(),
		"source_path":    sourceRepository.GetRelativePath(),
		"target_storage": targetRepository.GetStorageName(),
		"target_path":    targetRepository.GetRelativePath(),
	})

	env, err := gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{Repository: sourceRepository})
	if err != nil {
		return nil, err
	}

	cmd, err := git.SafeBareCmd(ctx, git.CmdStream{}, env, nil,
		git.SubCmd{
			Name:        "clone",
			Flags:       flags,
			Args:        args,
			PostSepArgs: postSepArgs,
		},
		git.WithRefTxHook(ctx, req.Repository, s.cfg),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: clone cmd start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: clone cmd wait: %v", err)
	}

	if err := s.removeOriginInRepo(ctx, targetRepository); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: %v", err)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: targetRepository}); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: create hooks failed: %v", err)
	}

	// The `git clone` creates an alternates file, but it uses an absolute path. We recreate
	// the file with a relative path.
	if objectPool != nil {
		if err := objectPool.ForceLink(ctx, targetRepository); err != nil {
			return nil, status.Errorf(codes.Internal, "CreateFork: error relinking object pool from target: %v", err)
		}
	}

	return &gitalypb.CreateForkResponse{}, nil
}

func validateCreateForkRequest(req *gitalypb.CreateForkRequest) error {
	targetRepository := req.Repository
	sourceRepository := req.SourceRepository

	if sourceRepository == nil {
		return errors.New("CreateFork: empty SourceRepository")
	}
	if targetRepository == nil {
		return errors.New("CreateFork: empty Repository")
	}

	poolRepository := req.GetPool().GetRepository()
	if poolRepository == nil {
		return nil
	}

	if targetRepository.GetStorageName() != poolRepository.GetStorageName() {
		return errors.New("target repository is on a different storage than the object pool")
	}

	return nil
}

func (s *server) getObjectPool(req *gitalypb.CreateForkRequest) (*objectpool.ObjectPool, string, error) {
	repository := req.GetPool().GetRepository()

	if repository == nil {
		return nil, "", nil
	}

	objectPool, err := objectpool.FromProto(s.cfg, config.NewLocator(s.cfg), req.GetPool())
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "CreateFork: get object pool from request: %v", err)
	}

	sourceObjectPoolPath, err := s.locator.GetPath(repository)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "CreateFork: unable to find source object pool: %v", err)
	}

	return objectPool, sourceObjectPoolPath, nil
}
