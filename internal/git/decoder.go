package git

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// RefsDecoder parses the output format of git-show-ref
type RefsDecoder struct {
	r   *bufio.Reader
	err error
}

// NewRefsDecoder returns a new RefsDecoder
func NewRefsDecoder(r io.Reader) *RefsDecoder {
	return &RefsDecoder{
		r: bufio.NewReader(r),
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

	splits := strings.SplitN(line, " ", 2)
	if len(splits) != 2 {
		if d.err != nil {
			return d.err
		}
		return fmt.Errorf("refs decoder: invalid line: %q", line)
	}

	*ref = NewReference(ReferenceName(splits[1]), splits[0])

	return nil
}
