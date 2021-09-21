package repository

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetConfig reads the repository's gitconfig file and returns its contents.
func (s *server) GetConfig(
	request *gitalypb.GetConfigRequest,
	stream gitalypb.RepositoryService_GetConfigServer,
) error {
	repoPath, err := s.locator.GetPath(request.GetRepository())
	if err != nil {
		return err
	}

	configPath := filepath.Join(repoPath, "config")

	gitconfig, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return status.Errorf(codes.NotFound, "opening gitconfig: %v", err)
		}
		return helper.ErrInternalf("opening gitconfig: %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetConfigResponse{
			Data: p,
		})
	})

	if _, err := io.Copy(writer, gitconfig); err != nil {
		return helper.ErrInternalf("sending config: %v", err)
	}

	return nil
}

func (s *server) voteOnConfig(ctx context.Context, repo *gitalypb.Repository) error {
	return transaction.RunOnContext(ctx, func(tx txinfo.Transaction) error {
		repoPath, err := s.locator.GetPath(repo)
		if err != nil {
			return fmt.Errorf("get repo path: %w", err)
		}

		var vote voting.Vote

		config, err := os.Open(filepath.Join(repoPath, "config"))
		switch {
		case err == nil:
			hash := voting.NewVoteHash()
			if _, err := io.Copy(hash, config); err != nil {
				return fmt.Errorf("seeding vote: %w", err)
			}

			vote, err = hash.Vote()
			if err != nil {
				return fmt.Errorf("computing vote: %w", err)
			}
		case os.IsNotExist(err):
			vote = voting.VoteFromData([]byte("notfound"))
		default:
			return fmt.Errorf("open repo config: %w", err)
		}

		if err := s.txManager.Vote(ctx, tx, vote); err != nil {
			return fmt.Errorf("casting vote: %w", err)
		}

		return nil
	})
}
