package repository

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	targetRepository := req.Repository
	sourceRepository := req.SourceRepository

	if sourceRepository == nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: empty SourceRepository")
	}
	if targetRepository == nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: empty Repository")
	}

	if err := s.createRepository(ctx, targetRepository, func(repo *gitalypb.Repository) error {
		targetPath, err := s.locator.GetPath(repo)
		if err != nil {
			return err
		}

		env, err := gitalyssh.UploadPackEnv(ctx, s.cfg, &gitalypb.SSHUploadPackRequest{Repository: sourceRepository})
		if err != nil {
			return err
		}

		// git-clone(1) doesn't allow for the target path to exist, so we have to
		// remove it first.
		if err := os.RemoveAll(targetPath); err != nil {
			return fmt.Errorf("removing target path: %w", err)
		}

		// Ideally we'd just fetch into the already-created repo, but that wouldn't
		// allow us to easily set up HEAD to point to the correct ref. We thus have
		// no easy choice but to use git-clone(1).
		cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--bare"},
			},
			Args: []string{
				gitalyssh.GitalyInternalURL,
				targetPath,
			},
		}, git.WithEnv(env...), git.WithDisabledHooks())
		if err != nil {
			return fmt.Errorf("spawning fetch: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("fetching source repo: %w", err)
		}

		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		return nil
	}); err != nil {
		return nil, helper.ErrInternalf("creating fork: %w", err)
	}

	return &gitalypb.CreateForkResponse{}, nil
}
