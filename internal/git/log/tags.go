package log

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// GetAllTags retrieves all tag objects for a given repository
func GetAllTags(ctx context.Context, repo *gitalypb.Repository) ([]*gitalypb.Tag, error) {
	var tags []*gitalypb.Tag

	c, err := catfile.New(ctx, repo)
	if err != nil {
		return tags, nil
	}

	tagsCmd, err := git.Command(ctx, repo, "for-each-ref", "--format", "%(objectname) %(refname:short)", "refs/tags/")
	if err != nil {
		return tags, err
	}
	s := bufio.NewScanner(tagsCmd)
	for s.Scan() {
		line := strings.SplitN(s.Text(), " ", 2)
		oid := strings.TrimSpace(line[0])
		info, err := c.Info(oid)
		if err != nil {
			return tags, err
		}
		tag, err := buildTag(oid, info.Type, line[1], c)
		if err != nil {
			return tags, err
		}
		tags = append(tags, tag)
	}

	return tags, nil
}

func buildTag(oid, objectType, tagName string, b *catfile.Batch) (*gitalypb.Tag, error) {
	var tag *gitalypb.Tag
	var err error
	switch objectType {
	// annotated tag
	case "tag":
		tag, err = buildAnnotatedTag(b, oid)
		if err != nil {
			return nil, err
		}
	// lightweight tag
	case "commit":
		tag, err = buildLightweightTag(b, oid, tagName)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown object type for tag")
	}
	return tag, nil
}

func buildAnnotatedTag(b *catfile.Batch, oid string) (*gitalypb.Tag, error) {
	r, err := b.Tag(oid)
	if err != nil {
		return nil, fmt.Errorf("buildAnnotatedTag error when getting tag reader: %v", err)
	}
	header, body, err := splitRawCommit(r)
	if err != nil {
		return nil, fmt.Errorf("buildAnnotatedTag error when splitting commit: %v", err)
	}

	tag := &gitalypb.Tag{
		Id:          oid,
		MessageSize: int64(len(body)),
		Message:     body,
	}

	if max := helper.MaxCommitOrTagMessageSize; len(body) > max {
		tag.Message = tag.Message[:max]
	}

	s := bufio.NewScanner(bytes.NewReader(header))
	for s.Scan() {
		line := s.Text()
		if len(line) == 0 || line[0] == ' ' {
			continue
		}
		headerSplit := strings.SplitN(line, " ", 2)
		switch headerSplit[0] {
		case "type":
			// we only want to get the referenced target commit, and are ignoring any other objects
			if headerSplit[1] != "commit" {
				continue
			}
		case "object":
			commit, err := GetCommitCatfile(b, headerSplit[1])
			if err != nil {
				return nil, fmt.Errorf("buildAnnotatedTag error when getting target commit: %v", err)
			}
			tag.TargetCommit = commit
		case "tag":
			tag.Name = []byte(headerSplit[1])
		case "tagger":
			tag.Tagger = parseCommitAuthor(headerSplit[1])
		}
	}

	return tag, nil
}

func buildLightweightTag(b *catfile.Batch, oid, name string) (*gitalypb.Tag, error) {
	commit, err := GetCommitCatfile(b, oid)
	if err != nil {
		return nil, err
	}

	tag := &gitalypb.Tag{
		Id:           oid,
		Name:         []byte(name),
		TargetCommit: commit,
	}

	return tag, nil
}
