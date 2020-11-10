package repository

import (
	"bytes"
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) Fsck(ctx context.Context, req *gitalypb.FsckRequest) (*gitalypb.FsckResponse, error) {
	var stdout, stderr bytes.Buffer

	repoPath, env, err := alternates.PathAndEnv(req.GetRepository())
	if err != nil {
		return nil, err
	}

	cmd, err := git.SafeBareCmd(ctx, git.CmdStream{Out: &stdout, Err: &stderr}, env,
		[]git.Option{git.ValueFlag{"--git-dir", repoPath}},
		git.SubCmd{Name: "fsck"},
	)
	if err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return &gitalypb.FsckResponse{Error: append(stdout.Bytes(), stderr.Bytes()...)}, nil
	}

	return &gitalypb.FsckResponse{}, nil
}
