package commit

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) CommitStats(ctx context.Context, in *gitalypb.CommitStatsRequest) (*gitalypb.CommitStatsResponse, error) {
	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	repo := in.GetRepository()
	if _, err := helper.GetRepoPath(repo); err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, repo)
	if err != nil {
		return nil, err
	}

	return client.CommitStats(clientCtx, in)
}
