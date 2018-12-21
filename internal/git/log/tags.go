package log

import (
	"bufio"
	"context"
	"errors"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

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
		if info.IsBlob() {
			continue
		}
		tag, err := buildTag(oid, info.Type, line[1], c)
		tags = append(tags, tag)
		if err != nil {
			return tags, err
		}
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
		return nil, err
	}
	tagBytes, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	header, body := splitRawCommit(tagBytes)
	headerLines := strings.Split(string(header), "\n")
	if err != nil {
		return nil, err
	}
	body = body[:len(body)-1]

	commit, err := GetCommitCatfile(b, strings.SplitN(headerLines[0], " ", 2)[1])
	if err != nil {
		return nil, err
	}
	tag := &gitalypb.Tag{
		Id:           oid,
		Name:         []byte(strings.SplitN(headerLines[2], " ", 2)[1]),
		Message:      body,
		MessageSize:  int64(len(body)),
		Tagger:       parseCommitAuthor(strings.SplitN(headerLines[3], " ", 2)[1]),
		TargetCommit: commit,
	}

	if max := helper.MaxCommitOrTagMessageSize; len(tag.Message) > max {
		tag.Message = tag.Message[:max]
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
