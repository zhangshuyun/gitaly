package catfile

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// GetTag looks up a commit by tagID using an existing catfile.Batch instance. Note: we pass
// in the tagName because the tag name from refs/tags may be different than the name found in the
// actual tag object. We want to use the tagName found in refs/tags
func GetTag(ctx context.Context, objectReader ObjectReader, tagID git.Revision, tagName string) (*gitalypb.Tag, error) {
	object, err := objectReader.Object(ctx, tagID)
	if err != nil {
		return nil, err
	}

	if object.Type != "tag" {
		if _, err := io.Copy(io.Discard, object); err != nil {
			return nil, fmt.Errorf("discarding object: %w", err)
		}

		return nil, NotFoundError{
			error: fmt.Errorf("expected tag, got %s", object.Type),
		}
	}

	tag, err := buildAnnotatedTag(ctx, objectReader, object, []byte(tagName))
	if err != nil {
		return nil, err
	}

	return tag, nil
}

// ExtractTagSignature extracts the signature from a content and returns both the signature
// and the remaining content. If no signature is found, nil as the signature and the entire
// content are returned. note: tags contain the signature block at the end of the message
// https://github.com/git/git/blob/master/Documentation/technical/signature-format.txt#L12
func ExtractTagSignature(content []byte) ([]byte, []byte) {
	index := bytes.Index(content, []byte("-----BEGIN"))

	if index > 0 {
		return bytes.TrimSuffix(content[index:], []byte("\n")), content[:index]
	}
	return nil, content
}

func buildAnnotatedTag(ctx context.Context, objectReader ObjectReader, object git.Object, name []byte) (*gitalypb.Tag, error) {
	tag, tagged, err := newParser().parseTag(object, name)
	if err != nil {
		return nil, err
	}

	switch tagged.objectType {
	case "commit":
		tag.TargetCommit, err = GetCommit(ctx, objectReader, git.Revision(tagged.objectID))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when getting target commit: %v", err)
		}

	case "tag":
		tag.TargetCommit, err = dereferenceTag(ctx, objectReader, git.Revision(tagged.objectID))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when dereferencing tag: %v", err)
		}
	}

	return tag, nil
}

// dereferenceTag recursively dereferences annotated tags until it finds a non-tag object. If it is
// a commit, then it will parse and return this commit. Otherwise, if the tagged object is not a
// commit, it will simply discard the object and not return an error.
func dereferenceTag(ctx context.Context, objectReader ObjectReader, oid git.Revision) (*gitalypb.GitCommit, error) {
	object, err := objectReader.Object(ctx, oid+"^{}")
	if err != nil {
		return nil, fmt.Errorf("peeling tag: %w", err)
	}

	switch object.Type {
	case "commit":
		return NewParser().ParseCommit(object)
	default: // This current tag points to a tree or a blob
		// We do not care whether discarding the object fails -- the worst that can
		// happen is that the object reader is dirty after the RPC call finishes,
		// and then we'll just create a new one when we require it again.
		_, _ = io.Copy(io.Discard, object)
		return nil, nil
	}
}

// TrimTagMessage trims the tag's message. The message length will be trimmed such that it fits into a
// single gRPC message and all trailing newline characters will be stripped.
func TrimTagMessage(tag *gitalypb.Tag) {
	// Remove trailing newline, if any, to preserve existing behavior the old GitLab tag finding code.
	// See https://gitlab.com/gitlab-org/gitaly/blob/5e94dc966ac1900c11794b107a77496552591f9b/ruby/lib/gitlab/git/repository.rb#L211.
	// Maybe this belongs in the FindAllTags handler, or even on the gitlab-ce client side, instead of here?
	tag.Message = bytes.TrimRight(tag.Message, "\n")

	// It is intentional that we set the message size _before_ truncating the commit but after
	// trimming trailing newlines: the caller likely doesn't care about trailing newlines, and
	// as such we shouldn't tell the caller that we did truncate the message by having differing
	// message length and message size. But we do want to tell the caller in case we truncated
	// the message, in which case message length and message size must be different.
	tag.MessageSize = int64(len(tag.Message))

	if max := helper.MaxCommitOrTagMessageSize; len(tag.Message) > max {
		tag.Message = tag.Message[:max]
	}
}
