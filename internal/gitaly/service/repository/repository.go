package repository

import (
	"context"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Deprecated
func (s *server) Exists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	return nil, helper.Unimplemented
}

func (s *server) RepositoryExists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	path, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryExistsResponse{Exists: storage.IsGitDirectory(path)}, nil
}

func (s *server) HasLocalBranches(ctx context.Context, in *gitalypb.HasLocalBranchesRequest) (*gitalypb.HasLocalBranchesResponse, error) {
	cmd, err := git.SafeCmd(ctx, in.GetRepository(), nil, git.SubCmd{Name: "for-each-ref", Flags: []git.Option{git.Flag{Name: "--count=1"}}, Args: []string{"refs/heads"}})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: gitCommand: %v", err)
	}

	buff, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: read: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "HasLocalBranches: cmd wait: %v", err)
	}

	return &gitalypb.HasLocalBranchesResponse{Value: len(buff) > 0}, nil
}
