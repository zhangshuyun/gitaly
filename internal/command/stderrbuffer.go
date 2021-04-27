package command

import (
	"bytes"
	"fmt"
)

const delimiter = '\n'

// stderrBuffer implements io.Writer and buffers outputs with limited buffer
// size and line length.
// Bytes will be truncated if `bufLimit` and `lineLimit` exceeded. It will
// always return the full len of input and nil on writes.
type stderrBuffer struct {
	buf       []byte
	bufLimit  int
	lineLimit int
	lineSep   []byte

	currentLineLength int
}

func newStderrBuffer(bufLimit, lineLimit int, lineSep []byte) (*stderrBuffer, error) {
	if bufLimit < 0 || lineLimit < 0 {
		return nil, fmt.Errorf("invalid limit")
	}
	res := &stderrBuffer{
		bufLimit:  bufLimit,
		lineLimit: lineLimit,
		lineSep:   lineSep,
	}
	if len(res.lineSep) == 0 {
		// use default '\n' for linesep if not specified
		res.lineSep = []byte{'\n'}
	}
	res.buf = make([]byte, 0, res.lineLimit)
	return res, nil
}

func (b *stderrBuffer) Write(p []byte) (int, error) {
	if b.bufLimit <= 0 || b.lineLimit <= 0 {
		return len(p), nil
	}
	// The loop below scans `p` for new lines and cares for lineLimit and bufLimit.
	// During a iteration
	// 1. if new line found, buffer the found line(or the last part of a line) and
	//    move cursor for next search
	// 2. if no new line found, buffer the rest of `p` and move cursor to the end
	s := 0 // search start index
	for s < len(p) && len(b.buf) < b.bufLimit {
		var part []byte
		var foundNewLine bool
		if i := bytes.IndexByte(p[s:], delimiter); i >= 0 {
			i += s
			part = p[s:i] // final '\n' not included
			s = i + 1
			foundNewLine = true
		} else {
			// no newLine found, we should try to buffer the rest of `p`
			part = p[s:]
			s = len(p)
		}

		// make line length limit and buf limit happy
		part = part[:min(len(part), b.lineLimit-b.currentLineLength, b.bufLimit-len(b.buf))]
		b.buf = append(b.buf, part...)

		if foundNewLine {
			// a new line found so we need to feed the final linesep for current line
			// and reset currentLineLength
			b.currentLineLength = 0
			if len(b.buf)+len(b.lineSep) <= b.bufLimit {
				b.buf = append(b.buf, b.lineSep...)
			} else {
				// not space anymore
				break
			}
		} else {
			b.currentLineLength += len(part)
		}
	}
	return len(p), nil
}

func (b *stderrBuffer) Len() int {
	return len(b.buf)
}

func (b *stderrBuffer) String() string {
	return string(b.buf)
}

func min(first int, candidates ...int) int {
	res := first
	for _, val := range candidates {
		if val < res {
			res = val
		}
	}
	return res
}
