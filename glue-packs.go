package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: glue-packs PACK1 < PACK2")
		os.Exit(1)
	}

	if err := _main(); err != nil {
		log.Fatal(err)
	}
}

func _main() error {
	pack1, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	defer pack1.Close()

	pack1Reader, err := NewPackReader(pack1)
	if err != nil {
		return err
	}

	nPack1 := pack1Reader.NumObjects()
	log.Printf("%s: %d objects", os.Args[1], nPack1)

	pack2Reader, err := NewPackReader(os.Stdin)
	if err != nil {
		return err
	}

	nPack2 := pack2Reader.NumObjects()
	log.Printf("stdin: %d objects", nPack2)

	summer := sha1.New()
	stdout := io.MultiWriter(os.Stdout, summer)

	if _, err := fmt.Fprint(stdout, packMagic); err != nil {
		return err
	}

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, nPack1+nPack2) // TODO check for overflow
	if _, err := stdout.Write(size); err != nil {
		return err
	}

	if _, err := io.Copy(stdout, pack1Reader); err != nil {
		return err
	}

	if _, err := io.Copy(stdout, pack2Reader); err != nil {
		return err
	}

	if _, err := stdout.Write(summer.Sum(nil)); err != nil {
		return err
	}

	return nil
}

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

func (pr *packReader) NumObjects() uint32 {
	return pr.numObjects
}

func (pr *packReader) Read(p []byte) (int, error) {
	// No data available? Try to read from pr.reader.
	if len(pr.avail) <= sumSize && pr.readErr == nil {
		copy(pr.buf[:], pr.avail)

		var nRead int
		nRead, pr.readErr = pr.reader.Read(pr.buf[len(pr.avail):])
		pr.avail = pr.buf[:len(pr.avail)+nRead]

		if nUncheckedBytes := len(pr.avail) - sumSize; nUncheckedBytes > 0 {
			if _, err := pr.sum.Write(pr.avail[:nUncheckedBytes]); err != nil {
				return 0, err
			}
		}

		if pr.readErr != nil && pr.readErr != io.EOF {
			return 0, pr.readErr
		}
	}

	nBytesAvailable := len(pr.avail) - sumSize

	if nBytesAvailable <= 0 {
		if pr.readErr == io.EOF && !bytes.Equal(pr.sum.Sum(nil), pr.avail) {
			return 0, fmt.Errorf("packfile checksum mismatch")
		}

		return 0, pr.readErr
	}

	nYielded := copy(p, pr.avail[:nBytesAvailable])
	pr.avail = pr.avail[nYielded:]
	return nYielded, nil
}
