package repository

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateRepositoryFromURL(ctx context.Context, req *gitalypb.CreateRepositoryFromURLRequest) (*gitalypb.CreateRepositoryFromURLResponse, error) {
	if err := validateCreateRepositoryFromURLRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateRepositoryFromURL: %v", err)
	}

	repository := req.Repository

	repositoryFullPath, err := helper.GetPath(repository)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(repositoryFullPath); !os.IsNotExist(err) {
		return nil, status.Errorf(codes.InvalidArgument, "CreateRepositoryFromURL: dest dir exists")
	}

	args := []string{
		"-c",
		"http.followRedirects=false",
		"clone",
		"--bare",
		"--",
		req.Url,
		repositoryFullPath,
	}
	cmd, err := git.CommandWithoutRepo(ctx, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: clone cmd start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		os.RemoveAll(repositoryFullPath)
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: clone cmd wait: %v", err)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repository}); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: create hooks failed: %v", err)
	}

	if err := removeOriginInRepo(ctx, repository); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: %v", err)
	}

	return &gitalypb.CreateRepositoryFromURLResponse{}, nil
}

func validateCreateRepositoryFromURLRequest(req *gitalypb.CreateRepositoryFromURLRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetUrl() == "" {
		return fmt.Errorf("empty Url")
	}

	return nil
}
