package lstree

import (
	"bufio"
	"errors"
	"fmt"
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

// NextEntry reads a tree entry as it would be written by `git ls-tree -z`.
func (p *Parser) NextEntry() (*Entry, error) {
	// Each tree entry is expected to have a format of `<mode> SP <type> SP <objectid> TAB <path> NUL`.

	treeEntryMode, err := p.reader.ReadBytes(' ')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("reading mode: %w", err)
	}
	treeEntryMode = treeEntryMode[:len(treeEntryMode)-1]

	treeEntryType, err := p.reader.ReadBytes(' ')
	if err != nil {
		return nil, fmt.Errorf("reading type: %w", err)
	}
	treeEntryType = treeEntryType[:len(treeEntryType)-1]

	treeEntryID, err := p.reader.ReadBytes('\t')
	if err != nil {
		return nil, fmt.Errorf("reading OID: %w", err)
	}
	treeEntryID = treeEntryID[:len(treeEntryID)-1]

	treeEntryPath, err := p.reader.ReadBytes(0x00)
	if err != nil {
		return nil, fmt.Errorf("reading path: %w", err)
	}
	treeEntryPath = treeEntryPath[:len(treeEntryPath)-1]

	objectType, err := toEnum(string(treeEntryType))
	if err != nil {
		return nil, err
	}

	objectID, err := git.NewObjectIDFromHex(string(treeEntryID))
	if err != nil {
		return nil, err
	}

	return &Entry{
		Mode:     treeEntryMode,
		Type:     objectType,
		ObjectID: objectID,
		Path:     string(treeEntryPath),
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
