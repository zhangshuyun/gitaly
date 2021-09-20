package ref

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func (s *server) FindAllTags(in *gitalypb.FindAllTagsRequest, stream gitalypb.RefService_FindAllTagsServer) error {
	ctx := stream.Context()

	if err := s.validateFindAllTagsRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	sortField, err := getTagSortField(in.GetSortBy())
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(in.GetRepository())

	if err := s.findAllTags(ctx, repo, sortField, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) findAllTags(ctx context.Context, repo *localrepo.Repo, sortField string, stream gitalypb.RefService_FindAllTagsServer) error {
	c, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return fmt.Errorf("error creating catfile: %v", err)
	}

	forEachRefIter := gitpipe.ForEachRef(ctx, repo, []string{"refs/tags/"}, sortField)
	forEachRefIter = gitpipe.RevisionTransform(ctx, forEachRefIter,
		func(r gitpipe.RevisionResult) []gitpipe.RevisionResult {
			// We transform the pipeline to include each tag-reference twice: once for
			// the "normal" object, and once we opportunistically peel the object to a
			// non-tag object. This is required such that we can efficiently parse the
			// tagged object.
			return []gitpipe.RevisionResult{
				r,
				{OID: r.OID + "^{}"},
			}
		},
	)

	catfileInfoIter := gitpipe.CatfileInfo(ctx, c, forEachRefIter)

	// In the previous pipeline step, we request information about both the object and the
	// peeled object in case the object is a tag. Given that we now know about object types, we
	// can filter out the second request in case the object is not a tag: peeling a non-tag
	// object to a non-tag object is always going to end up with the same object anyway. And
	// requesting the same object twice is moot.
	type state int
	const (
		// stateTag indicates that the next object is going to be a tag.
		stateTag = state(iota)
		// statePeeledTag indicates that the next object is going to be the peeled object of
		// the preceding tag.
		statePeeledTag
		// stateSkip indicates that the next object shall be skipped because it is the
		// peeled version of a non-tag object, which is the same object anyway.
		stateSkip
	)

	currentState := stateTag
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter,
		func(r gitpipe.CatfileInfoResult) bool {
			switch currentState {
			case stateTag:
				// If we've got a tag, then we want to also see its peeled object.
				// Otherwise, we can skip over the peeled object.
				currentState = statePeeledTag
				if r.ObjectInfo.Type != "tag" {
					currentState = stateSkip
				}
				return true
			case statePeeledTag:
				currentState = stateTag
				return true
			case stateSkip:
				currentState = stateTag
				return false
			}

			// We could try to gracefully handle this, but I don't see much of a point
			// given that we can see above that it's never going to be anything else but
			// a known state.
			panic("invalid state")
		},
	)

	catfileObjectsIter := gitpipe.CatfileObject(ctx, c, catfileInfoIter)

	chunker := chunk.New(&tagSender{stream: stream})

	for catfileObjectsIter.Next() {
		tag := catfileObjectsIter.Result()

		var result *gitalypb.Tag
		switch tag.ObjectInfo.Type {
		case "tag":
			var err error
			result, err = catfile.ParseTag(tag.ObjectReader, tag.ObjectInfo.Oid)
			if err != nil {
				return fmt.Errorf("parsing annotated tag: %w", err)
			}

			// For each tag, we expect both the tag itself as well as its
			// potentially-peeled tagged object.
			if !catfileObjectsIter.Next() {
				return errors.New("expected peeled tag")
			}

			peeledTag := catfileObjectsIter.Result()

			// We only need to parse the tagged object in case we have an annotated tag
			// which refers to a commit object. Otherwise, we discard the object's
			// contents.
			if peeledTag.ObjectInfo.Type == "commit" {
				result.TargetCommit, err = catfile.ParseCommit(peeledTag.ObjectReader, peeledTag.ObjectInfo.Oid)
				if err != nil {
					return fmt.Errorf("parsing tagged commit: %w", err)
				}
			} else {
				if _, err := io.Copy(io.Discard, peeledTag.ObjectReader); err != nil {
					return fmt.Errorf("discarding tagged object contents: %w", err)
				}
			}
		case "commit":
			commit, err := catfile.ParseCommit(tag.ObjectReader, tag.ObjectInfo.Oid)
			if err != nil {
				return fmt.Errorf("parsing tagged commit: %w", err)
			}

			result = &gitalypb.Tag{
				Id:           tag.ObjectInfo.Oid.String(),
				TargetCommit: commit,
			}
		default:
			if _, err := io.Copy(io.Discard, tag.ObjectReader); err != nil {
				return fmt.Errorf("discarding tag object contents: %w", err)
			}

			result = &gitalypb.Tag{
				Id: tag.ObjectInfo.Oid.String(),
			}
		}

		// In case we can deduce the tag name from the object name (which should typically
		// be the case), we always want to return the tag name. While annotated tags do have
		// their name encoded in the object itself, we instead want to default to the name
		// of the reference such that we can discern multiple refs pointing to the same tag.
		if tagName := bytes.TrimPrefix(tag.ObjectName, []byte("refs/tags/")); len(tagName) > 0 {
			result.Name = tagName
		}

		if err := chunker.Send(result); err != nil {
			return fmt.Errorf("sending tag: %w", err)
		}
	}

	if err := catfileObjectsIter.Err(); err != nil {
		return fmt.Errorf("iterating over tags: %w", err)
	}

	if err := chunker.Flush(); err != nil {
		return fmt.Errorf("flushing chunker: %w", err)
	}

	return nil
}

func (s *server) validateFindAllTagsRequest(request *gitalypb.FindAllTagsRequest) error {
	if request.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if _, err := s.locator.GetRepoPath(request.GetRepository()); err != nil {
		return fmt.Errorf("invalid git directory: %v", err)
	}

	return nil
}

type tagSender struct {
	tags   []*gitalypb.Tag
	stream gitalypb.RefService_FindAllTagsServer
}

func (t *tagSender) Reset() {
	t.tags = t.tags[:0]
}

func (t *tagSender) Append(m proto.Message) {
	t.tags = append(t.tags, m.(*gitalypb.Tag))
}

func (t *tagSender) Send() error {
	return t.stream.Send(&gitalypb.FindAllTagsResponse{
		Tags: t.tags,
	})
}
