package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// WriteCommitGraph write or update commit-graph file in a repository
func (s *server) WriteCommitGraph(
	ctx context.Context,
	in *gitalypb.WriteCommitGraphRequest,
) (*gitalypb.WriteCommitGraphResponse, error) {
	repo := s.localrepo(in.GetRepository())

	if err := writeCommitGraph(ctx, repo, in.GetSplitStrategy()); err != nil {
		return nil, err
	}

	return &gitalypb.WriteCommitGraphResponse{}, nil
}

func writeCommitGraph(
	ctx context.Context,
	repo *localrepo.Repo,
	splitStrategy gitalypb.WriteCommitGraphRequest_SplitStrategy,
) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	missingBloomFilters := true
	if _, err := os.Stat(filepath.Join(repoPath, stats.CommitGraphRelPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return helper.ErrInternal(fmt.Errorf("remove commit graph file: %w", err))
		}

		// objects/info/commit-graph file doesn't exists
		// check if commit-graph chain exists and includes Bloom filters
		if missingBloomFilters, err = stats.IsMissingBloomFilters(repoPath); err != nil {
			return helper.ErrInternal(fmt.Errorf("should remove commit graph chain: %w", err))
		}
	}

	flags := []git.Option{
		git.Flag{Name: "--reachable"},
		git.Flag{Name: "--changed-paths"}, // enables Bloom filters
	}

	if missingBloomFilters {
		// if commit graph doesn't use Bloom filters we instruct operation to replace
		// existent commit graph with the new one
		// https://git-scm.com/docs/git-commit-graph#Documentation/git-commit-graph.txt-emwriteem
		flags = append(flags, git.Flag{Name: "--split=replace"})
	} else {
		flags = append(flags, git.Flag{Name: "--split"})
	}

	switch splitStrategy {
	case gitalypb.WriteCommitGraphRequest_SizeMultiple:
		flags = append(flags,
			// this flag has no effect if '--split=replace' is used
			git.ValueFlag{Name: "--size-multiple", Value: "4"},
		)
	default:
		return helper.ErrInvalidArgumentf("unsupported split strategy: %v", splitStrategy)
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "commit-graph",
		Action: "write",
		Flags:  flags,
	}, git.WithStderr(&stderr)); err != nil {
		return helper.ErrInternalf("writing commit-graph: %s: %v", err, stderr.String())
	}

	return nil
}
