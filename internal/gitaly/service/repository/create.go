package repository

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateRepository(ctx context.Context, req *gitalypb.CreateRepositoryRequest) (*gitalypb.CreateRepositoryResponse, error) {
	diskPath, err := s.locator.GetPath(req.GetRepository())
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("locate repository: %w", err)
	}

	if err := os.MkdirAll(diskPath, 0770); err != nil {
		return nil, helper.ErrInternalf("create directories: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "init",
			Flags: []git.Option{
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--quiet"},
			},
			Args: []string{diskPath},
		},
		git.WithStderr(stderr),
	)
	if err != nil {
		return nil, helper.ErrInternalf("create git init: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, helper.ErrInternalf("git init stderr: %q, err: %w", stderr, err)
	}

	// Given that git-init(1) does not create any refs, we never cast a vote on it. We thus do
	// manual voting here by hashing all references of the repository. While this would in the
	// general case hash nothing given that no refs exist yet, due to the idempotency of this
	// RPC it may be that we already do have some preexisting refs (e.g. CreateRepository is
	// called for a repo which already exists and has refs). In that case, voting ensures that
	// all replicas have the same set of preexisting refs.
	if err := transaction.RunOnContext(ctx, func(tx txinfo.Transaction, server txinfo.PraefectServer) error {
		hash := voting.NewVoteHash()

		cmd, err := s.gitCmdFactory.New(ctx, req.GetRepository(), git.SubCmd{
			Name: "for-each-ref",
		}, git.WithStdout(hash))
		if err != nil {
			return fmt.Errorf("for-each-ref: %v", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("waiting for for-each-ref: %v", err)
		}

		vote, err := hash.Vote()
		if err != nil {
			return err
		}

		if err := s.txManager.Vote(ctx, tx, server, vote); err != nil {
			return fmt.Errorf("casting vote: %w", err)
		}

		return nil
	}); err != nil {
		return nil, status.Errorf(codes.Aborted, "vote failed after initializing repo: %v", err)
	}

	return &gitalypb.CreateRepositoryResponse{}, nil
}
