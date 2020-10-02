package repository

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (s *server) FetchSourceBranch(ctx context.Context, req *gitalypb.FetchSourceBranchRequest) (*gitalypb.FetchSourceBranchResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoFetchSourceBranch) {
		return s.rubyFetchSourceBranch(ctx, req)
	}

	if err := git.ValidateRevision(req.GetSourceBranch()); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if err := git.ValidateRevision(req.GetTargetRef()); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return nil, err
	}

	refspec := fmt.Sprintf("%s:%s", req.GetSourceBranch(), req.GetTargetRef())

	var remote string
	var env []string
	if helper.RepoPathEqual(req.GetRepository(), req.GetSourceRepository()) {
		remote = "file://" + repoPath
	} else {
		remote = gitalyInternalURL
		env, err = gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{Repository: req.SourceRepository})
		if err != nil {
			return nil, err
		}
	}

	cmd, err := git.SafeBareCmd(ctx, git.CmdStream{}, env,
		[]git.Option{git.ValueFlag{"--git-dir", repoPath}},
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--prune"}},
			Args:  []string{remote, refspec},
		},
	)
	if err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		// Design quirk: if the fetch fails, this RPC returns Result: false, but no error.
		ctxlogrus.Extract(ctx).
			WithField("repo_path", repoPath).
			WithField("remote", remote).
			WithField("refspec", refspec).
			WithError(err).Warn("git fetch failed")
		return &gitalypb.FetchSourceBranchResponse{Result: false}, nil
	}

	return &gitalypb.FetchSourceBranchResponse{Result: true}, nil
}

func (s *server) rubyFetchSourceBranch(ctx context.Context, req *gitalypb.FetchSourceBranchRequest) (*gitalypb.FetchSourceBranchResponse, error) {
	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FetchSourceBranch(clientCtx, req)
}
