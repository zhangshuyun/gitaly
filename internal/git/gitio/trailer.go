package gitio

import (
	"fmt"
	"io"
)

type TrailerReader struct {
	r           io.Reader
	start, end  int
	trailerSize int
	buf         []byte
	atEOF       bool
}

func NewTrailerReader(r io.Reader, trailerSize int) *TrailerReader {
	bufSize := 4096
	for 2*trailerSize > bufSize {
		bufSize *= 2
	}

	return &TrailerReader{
		r:           r,
		trailerSize: trailerSize,
		buf:         make([]byte, bufSize),
	}
}

func (tr *TrailerReader) Trailer() ([]byte, error) {
	bufLen := tr.end - tr.start
	if !tr.atEOF || bufLen > tr.trailerSize {
		return nil, fmt.Errorf("cannot get trailer before reader has reached EOF")
	}

	if bufLen < tr.trailerSize {
		return nil, fmt.Errorf("not enough bytes to yield trailer")
	}

	return tr.buf[tr.end-tr.trailerSize : tr.end], nil
}

func (tr *TrailerReader) Read(p []byte) (int, error) {
	if bufLen := tr.end - tr.start; !tr.atEOF && bufLen <= tr.trailerSize {
		copy(tr.buf, tr.buf[tr.start:tr.end])
		tr.start = 0
		tr.end = bufLen

		n, err := tr.r.Read(tr.buf[tr.end:])
		if err != nil {
			if err != io.EOF {
				return 0, err
			}
			tr.atEOF = true
		}
		tr.end += n
	}

	if tr.end-tr.start <= tr.trailerSize {
		if tr.atEOF {
			return 0, io.EOF
		}
		return 0, nil
	}

	chunk := tr.end - tr.start - tr.trailerSize
	if chunk > len(p) {
		chunk = len(p)
	}

	copy(p, tr.buf[tr.start:tr.start+chunk])
	tr.start += chunk
	return chunk, nil
}
