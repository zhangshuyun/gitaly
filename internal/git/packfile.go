package git

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

type PackReader struct {
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
func NewPackReader(r io.Reader) (*PackReader, error) {
	pr := &PackReader{
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

func (pr *PackReader) NumObjects() uint32 { return pr.numObjects }

func (pr *PackReader) numBytesAvailable() int { return len(pr.avail) - sumSize }

func (pr *PackReader) Read(p []byte) (int, error) {
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

type PackWriter struct {
	w       io.Writer
	summer  hash.Hash
	flushed bool
}

// NewWriter creates a new PackWriter, writes its header, and returns the
// PackWriter. The caller must call Flush() when done, or else the
// packfile written to w will be invalid.
func NewPackWriter(w io.Writer, numObjects uint32) (*PackWriter, error) {
	pw := &PackWriter{
		summer: sha1.New(),
	}
	pw.w = io.MultiWriter(w, pw.summer)

	if _, err := pw.w.Write([]byte(packMagic)); err != nil {
		return nil, err
	}

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, numObjects)
	if _, err := pw.w.Write(size); err != nil {
		return nil, err
	}

	return pw, nil
}

type alreadyFlushedError struct{}

func (alreadyFlushedError) Error() string { return "PackWriter already flushed" }

func (pw *PackWriter) Write(p []byte) (int, error) {
	if pw.flushed {
		return 0, alreadyFlushedError{}
	}

	return pw.w.Write(p)
}

// Flush finalizes the packfile by writing its trailing checksum.
func (pw *PackWriter) Flush() error {
	if pw.flushed {
		return alreadyFlushedError{}
	}
	pw.flushed = true

	sum := pw.summer.Sum(nil)

	// Feeding the checksum back into pw.w messes up the state of pw.summer
	// but we will not use it again so it's OK.
	_, err := pw.w.Write(sum)
	return err
}
