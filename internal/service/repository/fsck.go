package repository

import (
	"bytes"

	"golang.org/x/net/context"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
)

func (s *server) Fsck(ctx context.Context, req *gitalypb.FsckRequest) (*gitalypb.FsckResponse, error) {
	var stdout, stderr bytes.Buffer

	repoPath, env, err := alternates.PathAndEnv(req.GetRepository())
	if err != nil {
		return nil, err
	}

	args := []string{"--git-dir", repoPath, "fsck"}

	cmd, err := git.BareCommand(ctx, nil, &stdout, &stderr, env, args...)
	if err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return &gitalypb.FsckResponse{Error: append(stdout.Bytes(), stderr.Bytes()...)}, nil
	}

	return &gitalypb.FsckResponse{}, nil
}
