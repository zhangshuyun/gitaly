package repository

import (
	"bytes"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"golang.org/x/net/context"
)

func (s *server) Fsck(ctx context.Context, req *gitalypb.FsckRequest) (*gitalypb.FsckResponse, error) {
	var stdout, stderr bytes.Buffer

	repoPath, env, err := alternates.PathAndEnv(req.GetRepository())
	if err != nil {
		return nil, err
	}

	args := []string{"--git-dir", repoPath, "fsck"}

	cmd, err := command.New(ctx, exec.Command(command.GitPath(), args...), nil, &stdout, &stderr, env...)
	if err != nil {
		return nil, err
	}

	if err = cmd.Wait(); err != nil {
		return &gitalypb.FsckResponse{Error: append(stdout.Bytes(), stderr.Bytes()...)}, nil
	}

	return &gitalypb.FsckResponse{}, nil
}
