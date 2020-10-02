package repository

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

const (
	CommitGraphRelPath = "objects/info/commit-graph"
)

// WriteCommitGraph write or update commit-graph file in a repository
func (*server) WriteCommitGraph(ctx context.Context, in *gitalypb.WriteCommitGraphRequest) (*gitalypb.WriteCommitGraphResponse, error) {
	if err := writeCommitGraph(ctx, in); err != nil {
		return nil, helper.ErrInternal(fmt.Errorf("WriteCommitGraph: gitCommand: %v", err))
	}

	return &gitalypb.WriteCommitGraphResponse{}, nil
}

func writeCommitGraph(ctx context.Context, in *gitalypb.WriteCommitGraphRequest) error {
	cmd, err := git.SafeCmd(ctx, in.GetRepository(), nil,
		git.SubCmd{
			Name: "commit-graph",
			Flags: []git.Option{
				git.SubSubCmd{Name: "write"},
				git.Flag{Name: "--reachable"},
			},
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}
