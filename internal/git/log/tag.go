package log

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// GetTagCatfile looks up a commit by tagID using an existing *catfile.Batch instance.
// note: we pass in the tagName because the tag name from refs/tags may be different
// than the name found in the actual tag object. We want to use the tagName found in refs/tags
func GetTagCatfile(c *catfile.Batch, tagID, tagName string) (*gitalypb.Tag, error) {
	r, err := c.Tag(tagID)
	if err != nil {
		return nil, err
	}

	header, body, err := splitRawTag(r)
	if err != nil {
		return nil, err
	}

	// the tagID is the oid of the tag object
	tag, err := buildAnnotatedTag(c, tagID, tagName, header, body)
	if err != nil {
		return nil, err
	}

	return tag, nil
}

type tagHeader struct {
	oid     string
	tagType string
}

func splitRawTag(r io.Reader) (*tagHeader, []byte, error) {
	raw, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}

	var body []byte
	split := bytes.SplitN(raw, []byte("\n\n"), 2)
	if len(split) == 2 {
		// Remove trailing newline, if any, to preserve existing behavior the old GitLab tag finding code.
		// See https://gitlab.com/gitlab-org/gitaly/blob/5e94dc966ac1900c11794b107a77496552591f9b/ruby/lib/gitlab/git/repository.rb#L211.
		// Maybe this belongs in the FindAllTags handler, or even on the gitlab-ce client side, instead of here?
		body = bytes.TrimRight(split[1], "\n")
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
		}
	}

	return &header, body, nil
}

func buildAnnotatedTag(b *catfile.Batch, tagID, name string, header *tagHeader, body []byte) (*gitalypb.Tag, error) {
	tag := &gitalypb.Tag{
		Id:          tagID,
		Name:        []byte(name),
		MessageSize: int64(len(body)),
		Message:     body,
	}

	if max := helper.MaxCommitOrTagMessageSize; len(body) > max {
		tag.Message = tag.Message[:max]
	}

	if header.tagType == "commit" {
		commit, err := GetCommitCatfile(b, header.oid)
		if err != nil {
			return nil, fmt.Errorf("buildAnnotatedTag error when getting target commit: %v", err)
		}

		tag.TargetCommit = commit
	}

	return tag, nil
}
