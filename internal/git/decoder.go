package git

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// RefsDecoder parses git output for git references
type RefsDecoder struct {
	r   *bufio.Reader
	sep string
	err error
}

// NewShowRefDecoder returns a new RefsDecoder to decode the output from git-show-ref
func NewShowRefDecoder(r io.Reader) *RefsDecoder {
	return &RefsDecoder{
		r:   bufio.NewReader(r),
		sep: " ",
	}
}

// Decode reads and parses the next reference. Returns io.EOF when the end of
// the reader has been reached.
func (d *RefsDecoder) Decode(ref *Reference) error {
	if d.err != nil {
		return d.err
	}

	line, err := d.r.ReadString('\n')
	d.err = err

	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}

	splits := strings.SplitN(line, d.sep, 2)
	if len(splits) != 2 {
		if d.err != nil {
			return d.err
		}
		return fmt.Errorf("refs decoder: invalid line: %q", line)
	}

	*ref = NewReference(ReferenceName(splits[1]), splits[0])

	return nil
}
