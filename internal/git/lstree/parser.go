package lstree

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// ErrParse is returned when the parse of an entry was unsuccessful
var ErrParse = errors.New("failed to parse git ls-tree response")

// Parser holds the necessary state for parsing the ls-tree output
type Parser struct {
	reader *bufio.Reader
}

// NewParser returns a new Parser
func NewParser(src io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(src),
	}
}

// NextEntry reads from git ls-tree --z --full-name command
// parses the tree entry and returns a *Entry.
func (p *Parser) NextEntry() (*Entry, error) {
	data, err := p.reader.ReadBytes(0x00)
	if err != nil {
		return nil, err
	}

	// We expect each `data` to be <mode> SP <type> SP <object> TAB <path>\0.
	split := bytes.SplitN(data, []byte(" "), 3)
	if len(split) != 3 {
		return nil, ErrParse
	}

	objectAndFile := bytes.SplitN(split[len(split)-1], []byte("\t"), 2)
	if len(objectAndFile) != 2 {
		return nil, ErrParse
	}

	objectType, err := toEnum(string(split[1]))
	if err != nil {
		return nil, err
	}

	// We know that the last byte in 'path' will be a zero byte.
	path := string(bytes.TrimRight(objectAndFile[1], "\x00"))

	objectID, err := git.NewObjectIDFromHex(string(objectAndFile[0]))
	if err != nil {
		return nil, err
	}

	return &Entry{
		Mode:     split[0],
		Type:     objectType,
		ObjectID: objectID,
		Path:     path,
	}, nil
}

func toEnum(s string) (ObjectType, error) {
	switch s {
	case "tree":
		return Tree, nil
	case "blob":
		return Blob, nil
	case "commit":
		return Submodule, nil
	default:
		return -1, ErrParse
	}
}
