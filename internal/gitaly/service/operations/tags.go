package operations

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserDeleteTag) {
		return s.UserDeleteTagRuby(ctx, req)
	}
	return s.UserDeleteTagGo(ctx, req)
}

func (s *server) UserDeleteTagRuby(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserDeleteTag(clientCtx, req)
}

func (s *server) UserDeleteTagGo(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if len(req.TagName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty tag name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	revision, err := git.NewRepository(req.Repository).GetReference(ctx, "refs/tags/"+string(req.TagName))
	if err != nil {
		return nil, helper.ErrPreconditionFailed(err)
	}

	tag := fmt.Sprintf("refs/tags/%s", req.TagName)

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, tag, git.NullSHA, revision.Name); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// TODO: Copy/pasted from branches.go. See
			// "When an error happens[...]". But I'm
			// returning the error, TODO: Need a test for
			// this.
			return &gitalypb.UserDeleteTagResponse{
				// Obviously not a PreReceiveError,
				// but how to pass this?
				PreReceiveError: updateRefError.reference,
			}, nil
		}

		return nil, err
	}

	return &gitalypb.UserDeleteTagResponse{}, nil
}

func (s *server) UserCreateTag(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	if featureflag.IsDisabled(ctx, featureflag.GoUserCreateTag) {
		return s.UserCreateTagRuby(ctx, req)
	}
	// TODO: Implement GoUserCreateTag
	return s.UserCreateTagGo(ctx, req)
}

func (s *server) UserCreateTagRuby(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.UserCreateTag(clientCtx, req)
}

// Relvant Ruby code:

//           repository: @gitaly_repo,
//           user: Gitlab::Git::User.from_gitlab(user).to_gitaly,
//           tag_name: encode_binary(tag_name),
//           target_revision: encode_binary(target),
//           message: encode_binary(message.to_s)

//     def user_create_tag(request, call)
//       repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
//       transaction = Praefect::Transaction.from_metadata(call.metadata)

//       gitaly_user = get_param!(request, :user)
//       user = Gitlab::Git::User.from_gitaly(gitaly_user)

//       tag_name = get_param!(request, :tag_name)

//       target_revision = get_param!(request, :target_revision)

//       created_tag = repo.add_tag(tag_name, user: user, target: target_revision, message: request.message.presence, transaction: transaction)
//       Gitaly::UserCreateTagResponse.new unless created_tag

//       rugged_commit = created_tag.dereferenced_target.rugged_commit
//       commit = gitaly_commit_from_rugged(rugged_commit)
//       tag = gitaly_tag_from_gitlab_tag(created_tag, commit)

//       Gitaly::UserCreateTagResponse.new(tag: tag)
//     rescue Gitlab::Git::Repository::InvalidRef => e
//       raise GRPC::FailedPrecondition.new(e.message)
//     rescue Gitlab::Git::Repository::TagExistsError
//       Gitaly::UserCreateTagResponse.new(exists: true)
//     rescue Gitlab::Git::PreReceiveError => e
//       Gitaly::UserCreateTagResponse.new(pre_receive_error: set_utf8!(e.message))
//     end


func (s *server) UserCreateTagGo(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	if len(req.TagName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty tag name)")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty user)")
	}

	// Note that "TargetRevision" isn't just a "type commit", we
	// also accept any other SHA-1 (tree, blob) when creating
	// tags.
	if len(req.TargetRevision) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty target revision)")
	}

	localRepo := git.NewRepository(req.Repository)
	targetOid, err := localRepo.ResolveRefish(ctx, string(req.TargetRevision))
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Bad Request (target does not exist)")
	}

	tag := fmt.Sprintf("refs/tags/%s", req.TagName)

	ourTagOid := ""
	if req.Message == nil {
		ourTagOid = targetOid
	} else {
		tagger := string(req.User.Name) + " <" + string(req.User.Email) + ">"
		// TODO: Need to support more than just "commit",
		// e.g. "blob", "tree". The web UI allows you to tag
		// trees at least, presumably blobs too. Needs tests.
		annotatedTagObj, err := localRepo.MkTag(ctx, targetOid, "commit", string(req.TagName), tagger, req.Message);
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		ourTagOid = annotatedTagObj
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, tag, ourTagOid, git.NullSHA); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserCreateTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			return &gitalypb.UserCreateTagResponse{
				Exists: true,
			}, nil
		}


		return nil, err
	}

	var tagObj *gitalypb.Tag
	if tagObj, err = ref.RawFindTag(ctx, req.Repository, req.TagName); err != nil {
		return nil, helper.ErrInternal(err)
	}
	return &gitalypb.UserCreateTagResponse{
		Tag: tagObj,
	}, nil
}
