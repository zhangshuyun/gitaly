package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) UserDeleteTag(ctx context.Context, req *gitalypb.UserDeleteTagRequest) (*gitalypb.UserDeleteTagResponse, error) {
	if len(req.TagName) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "empty tag name")
	}

	if req.User == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty user")
	}

	referenceName := git.ReferenceName(fmt.Sprintf("refs/tags/%s", req.TagName))
	revision, err := s.localrepo(req.GetRepository()).ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "tag not found: %s", req.TagName)
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, nil, referenceName, git.ZeroOID, revision); err != nil {
		var preReceiveError updateref.PreReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteTagResponse{
				PreReceiveError: preReceiveError.Message,
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}

		return nil, err
	}

	return &gitalypb.UserDeleteTagResponse{}, nil
}

func validateUserCreateTag(req *gitalypb.UserCreateTagRequest) error {
	// Emulate validations done by Ruby. A lot of these (e.g. the
	// upper-case error messages) can be simplified once we're not
	// doing bug-for-bug Ruby emulation anymore)
	if len(req.TagName) == 0 {
		return status.Errorf(codes.InvalidArgument, "empty tag name")
	}

	if bytes.Contains(req.TagName, []byte(" ")) {
		return status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", req.TagName)
	}

	if req.User == nil {
		return status.Errorf(codes.InvalidArgument, "empty user")
	}

	if len(req.TargetRevision) == 0 {
		return status.Error(codes.InvalidArgument, "empty target revision")
	}

	if bytes.Contains(req.Message, []byte("\000")) {
		return status.Errorf(codes.Unknown, "ArgumentError: string contains null byte")
	}

	// Our own Go-specific validation
	if req.GetRepository() == nil {
		return status.Errorf(codes.Internal, "empty repository")
	}

	return nil
}

func (s *Server) UserCreateTag(ctx context.Context, req *gitalypb.UserCreateTagRequest) (*gitalypb.UserCreateTagResponse, error) {
	// Validate the request
	if err := validateUserCreateTag(req); err != nil {
		return nil, err
	}

	targetRevision := git.Revision(req.TargetRevision)

	committerTime := time.Now()
	if req.Timestamp != nil {
		var err error
		committerTime, err = ptypes.Timestamp(req.Timestamp)
		if err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}
	}

	var quarantineRepo *localrepo.Repo
	var quarantineDir *quarantine.Dir
	if featureflag.QuarantinedUserCreateTag.IsEnabled(ctx) {
		var err error
		quarantineDir, err = quarantine.New(ctx, req.GetRepository(), s.locator)
		if err != nil {
			return nil, helper.ErrInternalf("creating object quarantine: %w", err)
		}

		quarantineRepo = s.localrepo(quarantineDir.QuarantinedRepo())
	} else {
		quarantineRepo = s.localrepo(req.GetRepository())
	}

	tag, tagID, err := s.createTag(ctx, quarantineRepo, targetRevision, req.TagName, req.Message, req.User, committerTime)
	if err != nil {
		return nil, err
	}

	referenceName := git.ReferenceName(fmt.Sprintf("refs/tags/%s", req.TagName))
	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, quarantineDir, referenceName, tagID, git.ZeroOID); err != nil {
		var preReceiveError updateref.PreReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserCreateTagResponse{
				PreReceiveError: preReceiveError.Message,
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			refNameOK, err := git.CheckRefFormat(ctx, s.gitCmdFactory, referenceName.String())
			if refNameOK {
				// The tag might not actually exist,
				// perhaps update-ref died for some
				// other reason. But saying that it
				// does is what the Ruby code used to
				// do, so let's follow it off that
				// cliff.
				return &gitalypb.UserCreateTagResponse{
					Tag:    nil,
					Exists: true,
				}, nil
			}

			var CheckRefFormatError git.CheckRefFormatError
			if errors.As(err, &CheckRefFormatError) {
				// It doesn't make sense either to
				// tell the user to retry with an
				// invalid ref name, but ditto on the
				// Ruby bug-for-bug emulation.
				return &gitalypb.UserCreateTagResponse{
					Tag:    nil,
					Exists: true,
				}, status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", req.TagName)
			}

			// Should only be reachable if "git
			// check-ref-format"'s invocation is
			// incorrect, or if it segfaults on startup
			// etc.
			return nil, status.Error(codes.Internal, err.Error())
		}

		// The Ruby code did not return this, but always an
		// "Exists: true" on update-ref failure without any
		// meaningful error. This is our "PANIC" response, if
		// we've got an unknown error (it should all be
		// updateRefError above) let's relay it to the
		// caller. This should not happen.
		return &gitalypb.UserCreateTagResponse{
			Tag:    nil,
			Exists: true,
		}, status.Error(codes.Internal, err.Error())
	}

	return &gitalypb.UserCreateTagResponse{
		Tag: tag,
	}, nil
}

func (s *Server) createTag(
	ctx context.Context,
	repo *localrepo.Repo,
	targetRevision git.Revision,
	tagName []byte,
	message []byte,
	committer *gitalypb.User,
	committerTime time.Time,
) (*gitalypb.Tag, git.ObjectID, error) {
	catFile, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return nil, "", status.Error(codes.Internal, err.Error())
	}

	// We allow all ways to name a revision that cat-file
	// supports, not just OID. Resolve it.
	targetInfo, err := catFile.Info(ctx, targetRevision)
	if err != nil {
		return nil, "", status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", targetRevision)
	}
	targetObjectID, targetObjectType := targetInfo.Oid, targetInfo.Type

	// Whether we have a "message" parameter tells us if we're
	// making a lightweight tag or an annotated tag. Maybe we can
	// use strings.TrimSpace() eventually, but as the tests show
	// the Ruby idea of Unicode whitespace is different than its
	// idea.
	makingTag := regexp.MustCompile(`\S`).Match(message)

	// If we're creating a tag to another "tag" we'll need to peel
	// (possibly recursively) all the way down to the
	// non-tag. That'll be our returned TargetCommit if it's a
	// commit (nil if tree or blob).
	//
	// If we're not pointing to a tag we pretend our "peeled" is
	// the commit/tree/blob object itself in subsequent logic.
	peeledTargetObjectID, peeledTargetObjectType := targetObjectID, targetObjectType
	if targetObjectType == "tag" {
		peeledTargetObjectInfo, err := catFile.Info(ctx, targetRevision+"^{}")
		if err != nil {
			return nil, "", status.Error(codes.Internal, err.Error())
		}
		peeledTargetObjectID, peeledTargetObjectType = peeledTargetObjectInfo.Oid, peeledTargetObjectInfo.Type

		// If we're not making an annotated tag ourselves and
		// the user pointed to a "tag" we'll ignore what they
		// asked for and point to what we found when peeling
		// that tag.
		if !makingTag {
			targetObjectID = peeledTargetObjectID
		}
	}

	// At this point we'll either be pointing to an object we were
	// provided with, or creating a new tag object and pointing to
	// that.
	refObjectID := targetObjectID
	var tagObject *gitalypb.Tag
	if makingTag {
		tagObjectID, err := repo.WriteTag(ctx, targetObjectID, targetObjectType, tagName, committer.Name, committer.Email, message, committerTime)
		if err != nil {
			var FormatTagError localrepo.FormatTagError
			if errors.As(err, &FormatTagError) {
				return nil, "", status.Errorf(codes.Unknown, "Rugged::InvalidError: failed to parse signature - expected prefix doesn't match actual")
			}

			var MktagError localrepo.MktagError
			if errors.As(err, &MktagError) {
				return nil, "", status.Errorf(codes.NotFound, "Gitlab::Git::CommitError: %s", err.Error())
			}
			return nil, "", status.Error(codes.Internal, err.Error())
		}

		createdTag, err := catfile.GetTag(ctx, catFile, tagObjectID.Revision(), string(tagName), false, false)
		if err != nil {
			return nil, "", status.Error(codes.Internal, err.Error())
		}

		tagObject = &gitalypb.Tag{
			Name:        tagName,
			Id:          tagObjectID.String(),
			Message:     createdTag.Message,
			MessageSize: createdTag.MessageSize,
			//TargetCommit: is filled in below if needed
		}

		refObjectID = tagObjectID
	} else {
		tagObject = &gitalypb.Tag{
			Name: tagName,
			Id:   peeledTargetObjectID.String(),
			//TargetCommit: is filled in below if needed
		}
	}

	// Save ourselves looking this up earlier in case update-ref
	// died
	if peeledTargetObjectType == "commit" {
		peeledTargetCommit, err := catfile.GetCommit(ctx, catFile, peeledTargetObjectID.Revision())
		if err != nil {
			return nil, "", status.Error(codes.Internal, err.Error())
		}
		tagObject.TargetCommit = peeledTargetCommit
	}

	return tagObject, refObjectID, nil
}
