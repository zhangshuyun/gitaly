package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
)

const (
	sumSize        = sha1.Size
	packBufferSize = 4096
)

type packReader struct {
	buf        [packBufferSize]byte
	avail      []byte
	reader     io.Reader
	readErr    error
	sum        hash.Hash
	numObjects uint32
}

const (
	packMagic      = "PACK\x00\x00\x00\x02"
	packHeaderSize = 12
)

// NewPackReader blocks until it has read the packfile header from r.
func NewPackReader(r io.Reader) (*packReader, error) {
	pr := &packReader{
		reader: r,
		sum:    sha1.New(),
	}

	header := make([]byte, packHeaderSize)
	if _, err := io.ReadFull(pr.reader, header); err != nil {
		return nil, err
	}

	if magic := string(header[:len(packMagic)]); magic != packMagic {
		return nil, fmt.Errorf("bad pack header: %q", magic)
	}

	pr.numObjects = binary.BigEndian.Uint32(header[len(packMagic):])

	if _, err := pr.sum.Write(header); err != nil {
		return nil, err
	}

	return pr, nil
}

func (pr *packReader) NumObjects() uint32 { return pr.numObjects }

func (pr *packReader) numBytesAvailable() int { return len(pr.avail) - sumSize }

func (pr *packReader) Read(p []byte) (int, error) {
	if pr.numBytesAvailable() <= 0 && pr.readErr == nil {
		copy(pr.buf[:], pr.avail)

		var nRead int
		nRead, pr.readErr = pr.reader.Read(pr.buf[len(pr.avail):])
		if pr.readErr != nil && pr.readErr != io.EOF {
			return 0, pr.readErr
		}

		pr.avail = pr.buf[:len(pr.avail)+nRead]

		if n := pr.numBytesAvailable(); n > 0 {
			if _, err := pr.sum.Write(pr.avail[:n]); err != nil {
				return 0, err
			}
		}
	}

	if pr.numBytesAvailable() <= 0 {
		if pr.readErr == io.EOF && !bytes.Equal(pr.sum.Sum(nil), pr.avail) {
			return 0, fmt.Errorf("packfile checksum mismatch")
		}

		return 0, pr.readErr
	}

	nYielded := copy(p, pr.avail[:pr.numBytesAvailable()])
	pr.avail = pr.avail[nYielded:]
	return nYielded, nil
}
