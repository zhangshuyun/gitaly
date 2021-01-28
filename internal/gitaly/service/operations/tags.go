package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	referenceName := fmt.Sprintf("refs/tags/%s", req.TagName)
	revision, err := localrepo.New(req.Repository, s.cfg).GetReference(ctx, git.ReferenceName(referenceName))
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "tag not found: %s", req.TagName)
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, git.ZeroOID.String(), revision.Target); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserDeleteTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
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

	// Setup
	repo := req.GetRepository()
	catFile, err := catfile.New(ctx, s.locator, repo)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// We allow all ways to name a revision that cat-file
	// supports, not just OID. Resolve it.
	targetRevision := git.Revision(req.TargetRevision)
	targetInfo, err := catFile.Info(ctx, targetRevision)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", targetRevision)
	}
	targetObjectID, targetObjectType := targetInfo.Oid, targetInfo.Type

	// Whether we have a "message" parameter tells us if we're
	// making a lightweight tag or an annotated tag. Maybe we can
	// use strings.TrimSpace() eventually, but as the tests show
	// the Ruby idea of Unicode whitespace is different than its
	// idea.
	makingTag := regexp.MustCompile(`\S`).Match(req.Message)

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
			return nil, status.Error(codes.Internal, err.Error())
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
		localRepo := localrepo.New(repo, s.cfg)

		committerTime := time.Now()
		if req.Timestamp != nil {
			committerTime, err = ptypes.Timestamp(req.Timestamp)
			if err != nil {
				return nil, helper.ErrInvalidArgument(err)
			}
		}

		tagObjectID, err := localRepo.WriteTag(ctx, targetObjectID, targetObjectType, req.TagName, req.User.Name, req.User.Email, req.Message, committerTime)
		if err != nil {
			var FormatTagError localrepo.FormatTagError
			if errors.As(err, &FormatTagError) {
				return nil, status.Errorf(codes.Unknown, "Rugged::InvalidError: failed to parse signature - expected prefix doesn't match actual")
			}

			var MktagError localrepo.MktagError
			if errors.As(err, &MktagError) {
				return nil, status.Errorf(codes.NotFound, "Gitlab::Git::CommitError: %s", err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}

		createdTag, err := log.GetTagCatfile(ctx, catFile, git.Revision(tagObjectID), string(req.TagName), false, false)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		tagObject = &gitalypb.Tag{
			Name:        req.TagName,
			Id:          tagObjectID,
			Message:     createdTag.Message,
			MessageSize: createdTag.MessageSize,
			//TargetCommit: is filled in below if needed
		}

		refObjectID = tagObjectID
	} else {
		tagObject = &gitalypb.Tag{
			Name: req.TagName,
			Id:   peeledTargetObjectID,
			//TargetCommit: is filled in below if needed
		}
	}

	referenceName := fmt.Sprintf("refs/tags/%s", req.TagName)
	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, referenceName, refObjectID, git.ZeroOID.String()); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserCreateTagResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			refNameOK, err := git.CheckRefFormat(ctx, referenceName)
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

	// Save ourselves looking this up earlier in case update-ref
	// died
	if peeledTargetObjectType == "commit" {
		peeledTargetCommit, err := log.GetCommitCatfile(ctx, catFile, git.Revision(peeledTargetObjectID))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		tagObject.TargetCommit = peeledTargetCommit
	}

	return &gitalypb.UserCreateTagResponse{
		Tag: tagObject,
	}, nil
}
