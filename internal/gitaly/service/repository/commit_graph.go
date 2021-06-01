package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	CommitGraphRelPath      = "objects/info/commit-graph"
	CommitGraphsRelPath     = "objects/info/commit-graphs"
	CommitGraphChainRelPath = CommitGraphsRelPath + "/commit-graph-chain"
)

// WriteCommitGraph write or update commit-graph file in a repository
func (s *server) WriteCommitGraph(
	ctx context.Context,
	in *gitalypb.WriteCommitGraphRequest,
) (*gitalypb.WriteCommitGraphResponse, error) {
	if err := s.writeCommitGraph(ctx, in.GetRepository(), in.GetSplitStrategy()); err != nil {
		return nil, err
	}

	return &gitalypb.WriteCommitGraphResponse{}, nil
}

func (s *server) writeCommitGraph(
	ctx context.Context,
	repo repository.GitRepo,
	splitStrategy gitalypb.WriteCommitGraphRequest_SplitStrategy,
) error {
	flags := []git.Option{
		git.Flag{Name: "--reachable"},
	}

	switch splitStrategy {
	case gitalypb.WriteCommitGraphRequest_SizeMultiple:
		flags = append(flags,
			git.Flag{Name: "--split"},
			git.ValueFlag{Name: "--size-multiple", Value: "4"},
		)
	default:
		return helper.ErrInvalidArgumentf("unsupported split strategy: %v", splitStrategy)
	}

	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "commit-graph",
			Action: "write",
			Flags:  flags,
		},
	)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
