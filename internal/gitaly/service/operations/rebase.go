package operations

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserRebaseConfirmable(stream gitalypb.OperationService_UserRebaseConfirmableServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return helper.ErrInvalidArgument(errors.New("UserRebaseConfirmable: empty UserRebaseConfirmableRequest.Header"))
	}

	if err := validateUserRebaseConfirmableHeader(header); err != nil {
		return helper.ErrInvalidArgumentf("UserRebaseConfirmable: %v", err)
	}

	ctx := stream.Context()

	repo := header.Repository
	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return err
	}

	branch := git.NewReferenceNameFromBranchName(string(header.Branch))
	oldrev, err := git.NewObjectIDFromHex(header.BranchSha)
	if err != nil {
		return helper.ErrNotFound(err)
	}

	remoteFetch := rebaseRemoteFetch{header: header}
	startRevision, err := s.fetchStartRevision(ctx, remoteFetch)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	committer := git2go.NewSignature(string(header.User.Name), string(header.User.Email), time.Now())
	if header.Timestamp != nil {
		committer.When, err = ptypes.Timestamp(header.Timestamp)
		if err != nil {
			return helper.ErrInvalidArgumentf("parse timestamp: %w", err)
		}
	}

	newrev, err := git2go.RebaseCommand{
		Repository:       repoPath,
		Committer:        committer,
		BranchName:       string(header.Branch),
		UpstreamRevision: startRevision.String(),
	}.Run(ctx, s.cfg)
	if err != nil {
		return stream.Send(&gitalypb.UserRebaseConfirmableResponse{
			GitError: err.Error(),
		})
	}

	if err := stream.Send(&gitalypb.UserRebaseConfirmableResponse{
		UserRebaseConfirmableResponsePayload: &gitalypb.UserRebaseConfirmableResponse_RebaseSha{
			RebaseSha: newrev.String(),
		},
	}); err != nil {
		return fmt.Errorf("send rebase sha: %w", err)
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("recv: %w", err)
	}

	if !secondRequest.GetApply() {
		return helper.ErrPreconditionFailedf("rebase aborted by client")
	}

	if err := s.updateReferenceWithHooks(
		ctx,
		header.Repository,
		header.User,
		branch,
		newrev,
		oldrev,
		header.GitPushOptions...); err != nil {
		switch {
		case errors.As(err, &updateref.PreReceiveError{}):
			return stream.Send(&gitalypb.UserRebaseConfirmableResponse{
				PreReceiveError: err.Error(),
			})
		case errors.Is(err, git2go.ErrInvalidArgument):
			return fmt.Errorf("update ref: %w", err)
		}

		return err
	}

	return stream.Send(&gitalypb.UserRebaseConfirmableResponse{
		UserRebaseConfirmableResponsePayload: &gitalypb.UserRebaseConfirmableResponse_RebaseApplied{
			RebaseApplied: true,
		},
	})
}

// ErrInvalidBranch indicates a branch name is invalid
var ErrInvalidBranch = errors.New("invalid branch name")

func validateUserRebaseConfirmableHeader(header *gitalypb.UserRebaseConfirmableRequest_Header) error {
	if header.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if header.GetUser() == nil {
		return errors.New("empty User")
	}

	if header.GetRebaseId() == "" {
		return errors.New("empty RebaseId")
	}

	if header.GetBranch() == nil {
		return errors.New("empty Branch")
	}

	if header.GetBranchSha() == "" {
		return errors.New("empty BranchSha")
	}

	if header.GetRemoteRepository() == nil {
		return errors.New("empty RemoteRepository")
	}

	if header.GetRemoteBranch() == nil {
		return errors.New("empty RemoteBranch")
	}

	if err := git.ValidateRevision(header.GetRemoteBranch()); err != nil {
		return ErrInvalidBranch
	}

	return nil
}

// rebaseRemoteFetch is an intermediate type that implements the
// `requestFetchingStartRevision` interface. This allows us to use
// `fetchStartRevision` to get the revision to rebase onto.
type rebaseRemoteFetch struct {
	header *gitalypb.UserRebaseConfirmableRequest_Header
}

func (r rebaseRemoteFetch) GetRepository() *gitalypb.Repository {
	return r.header.GetRepository()
}

func (r rebaseRemoteFetch) GetBranchName() []byte {
	return r.header.GetBranch()
}

func (r rebaseRemoteFetch) GetStartRepository() *gitalypb.Repository {
	return r.header.GetRemoteRepository()
}

func (r rebaseRemoteFetch) GetStartBranchName() []byte {
	return r.header.GetRemoteBranch()
}
