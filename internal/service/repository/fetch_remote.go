package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
)

func (s *server) FetchRemote(ctx context.Context, in *gitalypb.FetchRemoteRequest) (*gitalypb.FetchRemoteResponse, error) {
	repo := in.GetRepository()

	force := ""
	if in.GetForce() {
		force = "--force"
	}

	prune := "--prune"
	if in.GetNoPrune() {
		prune = ""
	}

	tags := "--tags"
	if in.GetNoTags() {
		tags = "--no-tags"
	}

	cmd, err := git.Command(ctx, repo, "fetch", in.GetRemote(), prune, force, tags, "--quiet")
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return &gitalypb.FetchRemoteResponse{}, nil

}
