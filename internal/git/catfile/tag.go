package catfile

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// MaxTagReferenceDepth is the maximum depth of tag references we will dereference
	MaxTagReferenceDepth = 10
)

// GetTag looks up a commit by tagID using an existing catfile.Batch instance. When 'trim' is
// 'true', the tag message will be trimmed to fit in a gRPC message. When 'trimRightNewLine' is
// 'true', the tag message will be trimmed to remove all '\n' characters from right. note: we pass
// in the tagName because the tag name from refs/tags may be different than the name found in the
// actual tag object. We want to use the tagName found in refs/tags
func GetTag(ctx context.Context, c Batch, tagID git.Revision, tagName string, trimLen, trimRightNewLine bool) (*gitalypb.Tag, error) {
	tagObj, err := c.Tag(ctx, tagID)
	if err != nil {
		return nil, err
	}

	header, body, err := splitRawTag(tagObj.Reader, trimRightNewLine)
	if err != nil {
		return nil, err
	}

	// the tagID is the oid of the tag object
	tag, err := buildAnnotatedTag(ctx, c, tagID.String(), tagName, header, body, trimLen, trimRightNewLine)
	if err != nil {
		return nil, err
	}

	return tag, nil
}

type tagHeader struct {
	oid     string
	tagType string
	tag     string
	tagger  string
}

func splitRawTag(r io.Reader, trimRightNewLine bool) (*tagHeader, []byte, error) {
	raw, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	var body []byte
	split := bytes.SplitN(raw, []byte("\n\n"), 2)
	if len(split) == 2 {
		body = split[1]
		if trimRightNewLine {
			// Remove trailing newline, if any, to preserve existing behavior the old GitLab tag finding code.
			// See https://gitlab.com/gitlab-org/gitaly/blob/5e94dc966ac1900c11794b107a77496552591f9b/ruby/lib/gitlab/git/repository.rb#L211.
			// Maybe this belongs in the FindAllTags handler, or even on the gitlab-ce client side, instead of here?
			body = bytes.TrimRight(body, "\n")
		}
	}

	var header tagHeader
	s := bufio.NewScanner(bytes.NewReader(split[0]))
	for s.Scan() {
		headerSplit := strings.SplitN(s.Text(), " ", 2)
		if len(headerSplit) != 2 {
			continue
		}

		key, value := headerSplit[0], headerSplit[1]
		switch key {
		case "object":
			header.oid = value
		case "type":
			header.tagType = value
		case "tag":
			header.tag = value
		case "tagger":
			header.tagger = value
		}
	}

	return &header, body, nil
}

func buildAnnotatedTag(ctx context.Context, b Batch, tagID, name string, header *tagHeader, body []byte, trimLen, trimRightNewLine bool) (*gitalypb.Tag, error) {
	tag := &gitalypb.Tag{
		Id:          tagID,
		Name:        []byte(name),
		MessageSize: int64(len(body)),
		Message:     body,
	}

	if max := helper.MaxCommitOrTagMessageSize; trimLen && len(body) > max {
		tag.Message = tag.Message[:max]
	}

	var err error
	switch header.tagType {
	case "commit":
		tag.TargetCommit, err = GetCommit(ctx, b, git.Revision(header.oid))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when getting target commit: %v", err)
		}

	case "tag":
		tag.TargetCommit, err = dereferenceTag(ctx, b, git.Revision(header.oid))
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when dereferencing tag: %v", err)
		}
	}

	// tags contain the signature block in the message:
	// https://github.com/git/git/blob/master/Documentation/technical/signature-format.txt#L12
	index := bytes.Index(body, []byte("-----BEGIN"))

	if index > 0 {
		length := bytes.Index(body[index:], []byte("\n"))

		if length > 0 {
			signature := string(body[index : length+index])
			tag.SignatureType = detectSignatureType(signature)
		}
	}

	tag.Tagger = parseCommitAuthor(header.tagger)

	return tag, nil
}

// dereferenceTag recursively dereferences annotated tags until it finds a commit.
// This matches the original behavior in the ruby implementation.
// we also protect against circular tag references. Even though this is not possible in git,
// we still want to protect against an infinite looop
func dereferenceTag(ctx context.Context, b Batch, oid git.Revision) (*gitalypb.GitCommit, error) {
	for depth := 0; depth < MaxTagReferenceDepth; depth++ {
		i, err := b.Info(ctx, oid)
		if err != nil {
			return nil, err
		}

		switch i.Type {
		case "tag":
			tagObj, err := b.Tag(ctx, oid)
			if err != nil {
				return nil, err
			}

			header, _, err := splitRawTag(tagObj.Reader, true)
			if err != nil {
				return nil, err
			}

			oid = git.Revision(header.oid)
			continue
		case "commit":
			return GetCommit(ctx, b, oid)
		default: // This current tag points to a tree or a blob
			return nil, nil
		}
	}

	// at this point the tag nesting has gone too deep. We want to return silently here however, as we don't
	// want to fail the entire request if one tag is nested too deeply.
	return nil, nil
}
