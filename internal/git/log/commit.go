package log

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/golang/protobuf/ptypes/timestamp"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// GetCommit tries to resolve revision to a Git commit. Returns nil if
// no object is found at revision.
func GetCommit(ctx context.Context, repo *gitalypb.Repository, revision string) (*gitalypb.GitCommit, error) {
	c, err := catfile.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return GetCommitCatfile(c, revision)
}

// GetCommitCatfile looks up a commit by revision using an existing *catfile.Batch instance.
func GetCommitCatfile(c *catfile.Batch, revision string) (*gitalypb.GitCommit, error) {
	info, err := c.Info(revision + "^{commit}")
	if err != nil {
		if catfile.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	r, err := c.Commit(info.Oid)
	if err != nil {
		return nil, err
	}

	return parseRawCommit(r, info)
}

// GetCommitMessage looks up a commit message and returns it in its entirety.
func GetCommitMessage(ctx context.Context, repo *gitalypb.Repository, revision string) (io.Reader, error) {
	c, err := catfile.New(ctx, repo)
	if err != nil {
		return nil, err
	}
	info, err := c.Info(revision + "^{commit}")
	if err != nil {
		if catfile.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	r, err := c.Commit(info.Oid)
	if err != nil {
		return nil, err
	}

	_, body := splitRawCommit(r)
	return body, nil
}

func parseRawCommit(r io.Reader, info *catfile.ObjectInfo) (*gitalypb.GitCommit, error) {
	header, body := splitRawCommit(r)
	return buildCommit(header, body, info)
}

func splitRawCommit(r io.Reader) (io.Reader, io.Reader) {
	var header bytes.Buffer
	bufReader := bufio.NewReader(r)
	for b, err := bufReader.ReadBytes('\n'); err == nil; {
		if string(b) == "\n" {
			break
		}
		header.Write(b)
		b, err = bufReader.ReadBytes('\n')
	}
	return &header, bufReader
}

func buildCommit(header, body io.Reader, info *catfile.ObjectInfo) (*gitalypb.GitCommit, error) {
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return &gitalypb.GitCommit{}, err
	}
	if len(bodyBytes) == 0 {
		bodyBytes = nil
	}

	commit := &gitalypb.GitCommit{
		Id:       info.Oid,
		BodySize: int64(len(bodyBytes)),
		Body:     bodyBytes,
		Subject:  subjectFromBody(bodyBytes),
	}

	if max := helper.MaxCommitOrTagMessageSize; len(bodyBytes) > max {
		commit.Body = commit.Body[:max]
	}

	scanner := bufio.NewScanner(header)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == ' ' {
			continue
		}

		headerSplit := strings.SplitN(line, " ", 2)
		if len(headerSplit) != 2 {
			continue
		}

		switch headerSplit[0] {
		case "parent":
			commit.ParentIds = append(commit.ParentIds, headerSplit[1])
		case "author":
			commit.Author = parseCommitAuthor(headerSplit[1])
		case "committer":
			commit.Committer = parseCommitAuthor(headerSplit[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return commit, nil
}

const maxUnixCommitDate = 1 << 53

func parseCommitAuthor(line string) *gitalypb.CommitAuthor {
	author := &gitalypb.CommitAuthor{}

	splitName := strings.SplitN(line, "<", 2)
	author.Name = []byte(strings.TrimSuffix(splitName[0], " "))

	if len(splitName) < 2 {
		return author
	}

	line = splitName[1]
	splitEmail := strings.SplitN(line, ">", 2)
	if len(splitEmail) < 2 {
		return author
	}

	author.Email = []byte(splitEmail[0])

	secSplit := strings.Fields(splitEmail[1])
	if len(secSplit) < 1 {
		return author
	}

	sec, err := strconv.ParseInt(secSplit[0], 10, 64)
	if err != nil || sec > maxUnixCommitDate || sec < 0 {
		sec = git.FallbackTimeValue.Unix()
	}

	author.Date = &timestamp.Timestamp{Seconds: sec}

	return author
}

func subjectFromBody(body []byte) []byte {
	return bytes.TrimRight(bytes.SplitN(body, []byte("\n"), 2)[0], "\r\n")
}
