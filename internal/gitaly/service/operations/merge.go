package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *Server) UserMergeBranch(bidi gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := bidi.Context()

	if featureflag.IsEnabled(ctx, featureflag.GoUserMergeBranch) {
		return s.userMergeBranch(bidi)
	}

	firstRequest, err := bidi.Recv()
	if err != nil {
		return err
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	rubyBidi, err := client.UserMergeBranch(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyBidi.Send(firstRequest); err != nil {
		return err
	}

	return rubyserver.ProxyBidi(
		func() error {
			request, err := bidi.Recv()
			if err != nil {
				return err
			}

			return rubyBidi.Send(request)
		},
		rubyBidi,
		func() error {
			response, err := rubyBidi.Recv()
			if err != nil {
				return err
			}

			return bidi.Send(response)
		},
	)
}

func validateMergeBranchRequest(request *gitalypb.UserMergeBranchRequest) error {
	if request.User == nil {
		return fmt.Errorf("empty user")
	}

	if len(request.Branch) == 0 {
		return fmt.Errorf("empty branch name")
	}

	if request.CommitId == "" {
		return fmt.Errorf("empty commit ID")
	}

	if len(request.Message) == 0 {
		return fmt.Errorf("empty message")
	}

	return nil
}

func (s *Server) userMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := firstRequest.Repository
	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return err
	}

	branch := "refs/heads/" + text.ChompBytes(firstRequest.Branch)
	revision, err := git.NewRepository(repo).ResolveRefish(ctx, branch)
	if err != nil {
		return err
	}

	merge, err := git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: string(firstRequest.User.Name),
		AuthorMail: string(firstRequest.User.Email),
		Message:    string(firstRequest.Message),
		Ours:       revision,
		Theirs:     firstRequest.CommitId,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return helper.ErrInvalidArgument(err)
		}
		return err
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		CommitId: merge.CommitID,
	}); err != nil {
		return err
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if !secondRequest.Apply {
		return helper.ErrPreconditionFailedf("merge aborted by client")
	}

	if err := s.updateReferenceWithHooks(ctx, firstRequest.Repository, firstRequest.User, branch, merge.CommitID, revision); err != nil {
		var preReceiveError preReceiveError
		var updateRefError updateRefError

		if errors.As(err, &preReceiveError) {
			err = stream.Send(&gitalypb.UserMergeBranchResponse{
				PreReceiveError: preReceiveError.message,
			})
		} else if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			err = stream.Send(&gitalypb.UserMergeBranchResponse{})
		}

		return err
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      merge.CommitID,
			RepoCreated:   false,
			BranchCreated: false,
		},
	}); err != nil {
		return err
	}

	return nil
}

func validateFFRequest(in *gitalypb.UserFFBranchRequest) error {
	if len(in.Branch) == 0 {
		return fmt.Errorf("empty branch name")
	}

	if in.User == nil {
		return fmt.Errorf("empty user")
	}

	if in.CommitId == "" {
		return fmt.Errorf("empty commit id")
	}

	return nil
}

func (s *Server) UserFFBranch(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	if err := validateFFRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if featureflag.IsDisabled(ctx, featureflag.GoUserFFBranch) {
		return s.userFFBranchRuby(ctx, in)
	}

	branch := fmt.Sprintf("refs/heads/%s", in.Branch)
	revision, err := git.NewRepository(in.Repository).ResolveRefish(ctx, branch)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	ancestor, err := isAncestor(ctx, in.Repository, revision, in.CommitId)
	if err != nil {
		return nil, err
	}
	if !ancestor {
		return nil, helper.ErrPreconditionFailedf("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.Repository, in.User, branch, in.CommitId, revision); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserFFBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserFFBranchResponse{}, nil
		}

		return nil, err
	}

	return &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId: in.CommitId,
		},
	}, nil
}

func (s *Server) userFFBranchRuby(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserFFBranch(clientCtx, in)
}

func validateUserMergeToRefRequest(in *gitalypb.UserMergeToRefRequest) error {
	if len(in.FirstParentRef) == 0 && len(in.Branch) == 0 {
		return fmt.Errorf("empty first parent ref and branch name")
	}

	if in.User == nil {
		return fmt.Errorf("empty user")
	}

	if in.SourceSha == "" {
		return fmt.Errorf("empty source SHA")
	}

	if len(in.TargetRef) == 0 {
		return fmt.Errorf("empty target ref")
	}

	if !strings.HasPrefix(string(in.TargetRef), "refs/merge-requests") {
		return fmt.Errorf("invalid target ref")
	}

	return nil
}

// userMergeToRef overwrites the given TargetRef to point to either Branch or
// FirstParentRef. Afterwards, it performs a merge of SourceSHA with either
// Branch or FirstParentRef and updates TargetRef to the merge commit.
func (s *Server) userMergeToRef(ctx context.Context, request *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return nil, err
	}

	repo := git.NewRepository(request.Repository)

	refName := string(request.Branch)
	if request.FirstParentRef != nil {
		refName = string(request.FirstParentRef)
	}

	ref, err := repo.ResolveRefish(ctx, refName)
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	sourceRef, err := repo.ResolveRefish(ctx, request.SourceSha)
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	// First, overwrite the reference with the target reference.
	if err := repo.UpdateRef(ctx, string(request.TargetRef), ref, ""); err != nil {
		return nil, updateRefError{reference: string(request.TargetRef)}
	}

	// Now, we create the merge commit...
	merge, err := git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: string(request.User.Name),
		AuthorMail: string(request.User.Email),
		Message:    string(request.Message),
		Ours:       ref,
		Theirs:     sourceRef,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		}
		//nolint:stylecheck
		return nil, helper.ErrPreconditionFailedf("Failed to create merge commit for source_sha %s and target_sha %s at %s", sourceRef, ref, string(request.TargetRef))
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, string(request.TargetRef), merge.CommitID, ref); err != nil {
		//nolint:stylecheck
		return nil, helper.ErrPreconditionFailed(fmt.Errorf("Could not update %s. Please refresh and try again", string(request.TargetRef)))
	}

	return &gitalypb.UserMergeToRefResponse{
		CommitId: merge.CommitID,
	}, nil
}

func (s *Server) UserMergeToRef(ctx context.Context, in *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	if err := validateUserMergeToRefRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	// Ruby has grown a new feature since being ported to Go, and we don't
	// handle that yet.
	if !in.AllowConflicts {
		return s.userMergeToRef(ctx, in)
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserMergeToRef(clientCtx, in)
}

func isAncestor(ctx context.Context, repo repository.GitRepo, ancestor, descendant string) (bool, error) {
	cmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name:  "merge-base",
		Flags: []git.Option{git.Flag{Name: "--is-ancestor"}},
		Args:  []string{ancestor, descendant},
	})
	if err != nil {
		return false, helper.ErrInternalf("isAncestor: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		status, ok := command.ExitStatus(err)
		if !ok {
			return false, helper.ErrInternalf("isAncestor: %w", err)
		}
		// --is-ancestor errors are signaled by a non-zero status that is not 1.
		// https://git-scm.com/docs/git-merge-base#Documentation/git-merge-base.txt---is-ancestor
		if status != 1 {
			return false, helper.ErrInvalidArgumentf("isAncestor: %w", err)
		}
		return false, nil
	}
	return true, nil
}
