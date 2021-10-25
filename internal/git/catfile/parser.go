package catfile

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Parser parses Git objects into their gitalypb representations.
type Parser interface {
	ParseCommit(object git.Object) (*gitalypb.GitCommit, error)
	ParseTag(object git.Object) (*gitalypb.Tag, error)
}

type parser struct {
	bufferedReader *bufio.Reader
}

// NewParser creates a new parser for Git objects.
func NewParser() Parser {
	return &parser{
		bufferedReader: bufio.NewReader(nil),
	}
}

// ParseCommit parses the commit data from the Reader.
func (p *parser) ParseCommit(object git.Object) (*gitalypb.GitCommit, error) {
	commit := &gitalypb.GitCommit{Id: object.ObjectID().String()}

	var lastLine bool
	p.bufferedReader.Reset(object)

	bytesRemaining := object.ObjectSize()
	for !lastLine {
		line, err := p.bufferedReader.ReadString('\n')
		if err == io.EOF {
			lastLine = true
		} else if err != nil {
			return nil, fmt.Errorf("parse raw commit: header: %w", err)
		}
		bytesRemaining -= int64(len(line))

		if len(line) == 0 || line[0] == ' ' {
			continue
		}
		// A blank line indicates the start of the commit body
		if line == "\n" {
			break
		}

		// There might not be a final line break if there was an EOF
		if line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
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
		case "gpgsig":
			commit.SignatureType = detectSignatureType(headerSplit[1])
		case "tree":
			commit.TreeId = headerSplit[1]
		}
	}

	if !lastLine {
		body := make([]byte, bytesRemaining)
		if _, err := io.ReadFull(p.bufferedReader, body); err != nil {
			return nil, fmt.Errorf("reading commit message: %w", err)
		}

		// After we have copied the body, we must make sure that there really is no
		// additional data. For once, this is to detect bugs in our implementation where we
		// would accidentally have truncated the commit message. On the other hand, we also
		// need to do this such that we observe the EOF, which we must observe in order to
		// unblock reading the next object.
		//
		// This all feels a bit complicated, where it would be much easier to just read into
		// a preallocated `bytes.Buffer`. But this complexity is indeed required to optimize
		// allocations. So if you want to change this, please make sure to execute the
		// `BenchmarkListAllCommits` benchmark.
		if n, err := io.Copy(io.Discard, p.bufferedReader); err != nil {
			return nil, fmt.Errorf("reading commit message: %w", err)
		} else if n != 0 {
			return nil, fmt.Errorf(
				"commit message exceeds expected length %v by %v bytes",
				object.ObjectSize(), n,
			)
		}

		if len(body) > 0 {
			commit.Subject = subjectFromBody(body)
			commit.BodySize = int64(len(body))
			commit.Body = body
			if max := helper.MaxCommitOrTagMessageSize; len(body) > max {
				commit.Body = commit.Body[:max]
			}
		}
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

	author.Date = &timestamppb.Timestamp{Seconds: sec}

	if len(secSplit) == 2 {
		author.Timezone = []byte(secSplit[1])
	}

	return author
}

func subjectFromBody(body []byte) []byte {
	return bytes.TrimRight(bytes.SplitN(body, []byte("\n"), 2)[0], "\r\n")
}

func detectSignatureType(line string) gitalypb.SignatureType {
	switch strings.TrimSuffix(line, "\n") {
	case "-----BEGIN SIGNED MESSAGE-----":
		return gitalypb.SignatureType_X509
	case "-----BEGIN PGP MESSAGE-----":
		return gitalypb.SignatureType_PGP
	case "-----BEGIN PGP SIGNATURE-----":
		return gitalypb.SignatureType_PGP
	default:
		return gitalypb.SignatureType_NONE
	}
}

// ParseTag parses the given object, which is expected to refer to a Git tag. The tag's tagged
// commit is not populated. The given object ID shall refer to the tag itself such that the returned
// Tag structure has the correct OID.
func (p *parser) ParseTag(object git.Object) (*gitalypb.Tag, error) {
	tag, _, err := parseTag(object, nil)
	return tag, err
}

type tagHeader struct {
	oid     string
	tagType string
	tag     string
	tagger  string
}

func parseTag(object git.Object, name []byte) (*gitalypb.Tag, *tagHeader, error) {
	header, body, err := splitRawTag(object)
	if err != nil {
		return nil, nil, err
	}

	if len(name) == 0 {
		name = []byte(header.tag)
	}

	tag := &gitalypb.Tag{
		Id:          object.ObjectID().String(),
		Name:        name,
		MessageSize: int64(len(body)),
		Message:     body,
	}

	signature, _ := ExtractTagSignature(body)
	if signature != nil {
		length := bytes.Index(signature, []byte("\n"))

		if length > 0 {
			signature := string(signature[:length])
			tag.SignatureType = detectSignatureType(signature)
		}
	}

	tag.Tagger = parseCommitAuthor(header.tagger)

	return tag, header, nil
}

func splitRawTag(object git.Object) (*tagHeader, []byte, error) {
	raw, err := io.ReadAll(object)
	if err != nil {
		return nil, nil, err
	}

	var body []byte
	split := bytes.SplitN(raw, []byte("\n\n"), 2)
	if len(split) == 2 {
		body = split[1]
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
