package repository

import (
	"context"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const fullPathKey = "gitlab.fullpath"

// SetFullPath writes the provided path value into the repository's gitconfig under the
// "gitlab.fullpath" key.
func (s *server) SetFullPath(
	ctx context.Context,
	request *gitalypb.SetFullPathRequest,
) (*gitalypb.SetFullPathResponse, error) {
	if request.GetRepository() == nil {
		return nil, helper.ErrInvalidArgumentf("empty Repository")
	}

	if len(request.GetPath()) == 0 {
		return nil, helper.ErrInvalidArgumentf("no path provided")
	}

	repo := s.localrepo(request.GetRepository())

	if featureflag.TxFileLocking.IsEnabled(ctx) {
		repoPath, err := repo.Path()
		if err != nil {
			return nil, helper.ErrInternalf("getting repository path: %w", err)
		}
		configPath := filepath.Join(repoPath, "config")

		writer, err := safe.NewLockingFileWriter(configPath, safe.LockingFileWriterConfig{
			SeedContents: true,
		})
		if err != nil {
			return nil, helper.ErrInternalf("creating config writer: %w", err)
		}
		defer writer.Close()

		if err := repo.ExecAndWait(ctx, git.SubCmd{
			Name: "config",
			Flags: []git.Option{
				git.Flag{Name: "--replace-all"},
				git.ValueFlag{Name: "--file", Value: writer.Path()},
			},
			Args: []string{fullPathKey, request.GetPath()},
		}); err != nil {
			return nil, helper.ErrInternalf("writing full path: %w", err)
		}

		if err := transaction.CommitLockedFile(ctx, s.txManager, writer); err != nil {
			return nil, helper.ErrInternalf("committing config: %w", err)
		}

		return &gitalypb.SetFullPathResponse{}, nil
	}

	if err := s.voteOnConfig(ctx, request.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on config: %w", err)
	}

	if err := repo.Config().Set(ctx, fullPathKey, request.GetPath()); err != nil {
		return nil, helper.ErrInternalf("writing config: %w", err)
	}

	if err := s.voteOnConfig(ctx, request.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on config: %w", err)
	}

	return &gitalypb.SetFullPathResponse{}, nil
}
