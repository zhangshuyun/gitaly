package gitio

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
)

type HashfileReader struct {
	tr  *TrailerReader
	tee io.Reader
	sum hash.Hash
}

func NewHashfileReader(r io.Reader) *HashfileReader {
	sum := sha1.New()
	tr := NewTrailerReader(r, sum.Size())
	return &HashfileReader{
		tr:  tr,
		tee: io.TeeReader(tr, sum),
		sum: sum,
	}
}

func (hr *HashfileReader) Read(p []byte) (int, error) {
	n, err := hr.tee.Read(p)
	if err == io.EOF {
		return n, hr.validateChecksum()
	}

	return n, err
}

func (hr *HashfileReader) validateChecksum() error {
	trailer, err := hr.tr.Trailer()
	if err != nil {
		return err
	}

	if actualSum := hr.sum.Sum(nil); !bytes.Equal(trailer, actualSum) {
		return fmt.Errorf("hashfile checksum mismatch: expected %x got %x", trailer, actualSum)
	}

	return io.EOF
}
