package operations

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

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

func (s *Server) UserMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
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

	referenceName := git.NewReferenceNameFromBranchName(string(firstRequest.Branch))

	revision, err := s.localrepo(repo).ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return err
	}

	authorDate, err := dateFromProto(firstRequest)
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	merge, err := git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: string(firstRequest.User.Name),
		AuthorMail: string(firstRequest.User.Email),
		AuthorDate: authorDate,
		Message:    string(firstRequest.Message),
		Ours:       revision.String(),
		Theirs:     firstRequest.CommitId,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return helper.ErrInvalidArgument(err)
		}
		return err
	}

	mergeOID, err := git.NewObjectIDFromHex(merge.CommitID)
	if err != nil {
		return helper.ErrInternalf("could not parse merge ID: %w", err)
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

	if err := s.updateReferenceWithHooks(ctx, firstRequest.Repository, firstRequest.User, referenceName, mergeOID, revision); err != nil {
		var preReceiveError updateref.PreReceiveError
		var updateRefError updateref.Error

		if errors.As(err, &preReceiveError) {
			err = stream.Send(&gitalypb.UserMergeBranchResponse{
				PreReceiveError: preReceiveError.Message,
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

	referenceName := git.NewReferenceNameFromBranchName(string(in.Branch))

	repo := s.localrepo(in.GetRepository())
	revision, err := repo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	commitID, err := git.NewObjectIDFromHex(in.CommitId)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("cannot parse commit ID: %w", err)
	}

	ancestor, err := repo.IsAncestor(ctx, revision.Revision(), commitID.Revision())
	if err != nil {
		return nil, err
	}
	if !ancestor {
		return nil, helper.ErrPreconditionFailedf("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.Repository, in.User, referenceName, commitID, revision); err != nil {
		var preReceiveError updateref.PreReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserFFBranchResponse{
				PreReceiveError: preReceiveError.Message,
			}, nil
		}

		var updateRefError updateref.Error
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

// UserMergeToRef overwrites the given TargetRef to point to either Branch or
// FirstParentRef. Afterwards, it performs a merge of SourceSHA with either
// Branch or FirstParentRef and updates TargetRef to the merge commit.
func (s *Server) UserMergeToRef(ctx context.Context, request *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	if err := validateUserMergeToRefRequest(request); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(request.GetRepository())

	revision := git.Revision(request.Branch)
	if request.FirstParentRef != nil {
		revision = git.Revision(request.FirstParentRef)
	}

	oid, err := repo.ResolveRevision(ctx, revision)
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(request.SourceSha))
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	authorDate, err := dateFromProto(request)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	// Resolve the current state of the target reference. We do not care whether it
	// exists or not, but what we do want to assert is that the target reference doesn't
	// change while we compute the merge commit as a small protection against races.
	var oldTargetOID git.ObjectID
	if targetRef, err := repo.GetReference(ctx, git.ReferenceName(request.TargetRef)); err == nil {
		if targetRef.IsSymbolic {
			return nil, helper.ErrPreconditionFailedf("target reference is symbolic: %q", request.TargetRef)
		}

		oid, err := git.NewObjectIDFromHex(targetRef.Target)
		if err != nil {
			return nil, helper.ErrInternalf("invalid target revision: %v", err)
		}

		oldTargetOID = oid
	} else if errors.Is(err, git.ErrReferenceNotFound) {
		oldTargetOID = git.ZeroOID
	} else {
		return nil, helper.ErrInternalf("could not read target reference: %v", err)
	}

	// Now, we create the merge commit...
	merge, err := git2go.MergeCommand{
		Repository:     repoPath,
		AuthorName:     string(request.User.Name),
		AuthorMail:     string(request.User.Email),
		AuthorDate:     authorDate,
		Message:        string(request.Message),
		Ours:           oid.String(),
		Theirs:         sourceOID.String(),
		AllowConflicts: request.AllowConflicts,
	}.Run(ctx, s.cfg)
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).WithFields(
			logrus.Fields{
				"source_sha": sourceOID,
				"target_sha": oid,
				"target_ref": string(request.TargetRef),
			},
		).Error("unable to create merge commit")

		if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrPreconditionFailedf("Failed to create merge commit for source_sha %s and target_sha %s at %s",
			sourceOID, oid, string(request.TargetRef))
	}

	mergeOID, err := git.NewObjectIDFromHex(merge.CommitID)
	if err != nil {
		return nil, err
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), mergeOID, oldTargetOID); err != nil {
		//nolint:stylecheck
		return nil, helper.ErrPreconditionFailed(fmt.Errorf("Could not update %s. Please refresh and try again", string(request.TargetRef)))
	}

	return &gitalypb.UserMergeToRefResponse{
		CommitId: mergeOID.String(),
	}, nil
}
