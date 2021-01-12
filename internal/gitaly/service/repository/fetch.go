package repository

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	targetRepo := git.NewRepository(req.GetRepository())

	sourceRepo, err := remoterepo.New(ctx, req.GetSourceRepository(), s.conns)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	var sourceOid string
	var containsObject bool

	// If source and target repository are the same, then we know that both
	// are local. We can thus optimize and locally resolve the reference
	// instead of using an RPC call. We also know that we can always skip
	// the fetch as the object will be available.
	if helper.RepoPathEqual(req.GetRepository(), req.GetSourceRepository()) {
		var err error

		sourceOid, err = targetRepo.ResolveRevision(ctx, git.Revision(req.GetSourceBranch()))
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return &gitalypb.FetchSourceBranchResponse{Result: false}, nil
			}
			return nil, helper.ErrInternal(err)
		}

		containsObject = true
	} else {
		var err error

		sourceOid, err = sourceRepo.ResolveRevision(ctx, git.Revision(req.GetSourceBranch()))
		if err != nil {
			if errors.Is(err, git.ErrReferenceNotFound) {
				return &gitalypb.FetchSourceBranchResponse{Result: false}, nil
			}
			return nil, helper.ErrInternal(err)
		}

		// Otherwise, if the source is a remote repository, we check
		// whether the target repo already contains the desired object.
		// If so, we can skip the fetch.
		containsObject, err = targetRepo.HasRevision(ctx, git.Revision(sourceOid+"^{commit}"))
		if err != nil {
			return nil, helper.ErrInternal(err)
		}
	}

	// There's no need to perform the fetch if we already have the object
	// available.
	if !containsObject {
		env, err := gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{Repository: req.SourceRepository})
		if err != nil {
			return nil, err
		}

		cmd, err := git.NewCommand(ctx, req.Repository, nil,
			git.SubCmd{
				Name:  "fetch",
				Args:  []string{gitalyssh.GitalyInternalURL, sourceOid},
				Flags: []git.Option{git.Flag{Name: "--no-tags"}, git.Flag{Name: "--quiet"}},
			},
			git.WithEnv(env...),
			git.WithRefTxHook(ctx, req.Repository, s.cfg),
		)
		if err != nil {
			return nil, err
		}
		if err := cmd.Wait(); err != nil {
			// Design quirk: if the fetch fails, this RPC returns Result: false, but no error.
			ctxlogrus.Extract(ctx).
				WithField("oid", sourceOid).
				WithError(err).Warn("git fetch failed")
			return &gitalypb.FetchSourceBranchResponse{Result: false}, nil
		}
	}

	if err := targetRepo.UpdateRef(ctx, git.ReferenceName(req.GetTargetRef()), sourceOid, ""); err != nil {
		return nil, err
	}

	return &gitalypb.FetchSourceBranchResponse{Result: true}, nil
}

func (s *server) rubyFetchSourceBranch(ctx context.Context, req *gitalypb.FetchSourceBranchRequest) (*gitalypb.FetchSourceBranchResponse, error) {
	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FetchSourceBranch(clientCtx, req)
}
