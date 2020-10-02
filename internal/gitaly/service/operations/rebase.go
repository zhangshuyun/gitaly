package operations

//lint:file-ignore SA1019 due to planned removal in issue https://gitlab.com/gitlab-org/gitaly/issues/1628

import (
	"errors"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (s *server) UserRebaseConfirmable(stream gitalypb.OperationService_UserRebaseConfirmableServer) error {
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

	if err := s.userRebaseConfirmable(stream, firstRequest, header.GetRepository()); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) userRebaseConfirmable(stream gitalypb.OperationService_UserRebaseConfirmableServer, firstRequest *gitalypb.UserRebaseConfirmableRequest, repository *gitalypb.Repository) error {
	ctx := stream.Context()
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, repository)
	if err != nil {
		return err
	}

	rubyStream, err := client.UserRebaseConfirmable(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	return rubyserver.ProxyBidi(
		func() error {
			request, err := stream.Recv()
			if err != nil {
				return err
			}

			return rubyStream.Send(request)
		},
		rubyStream,
		func() error {
			response, err := rubyStream.Recv()
			if err != nil {
				return err
			}

			return stream.Send(response)
		},
	)
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
