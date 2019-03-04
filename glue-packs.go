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

const packMagic = "PACK\x00\x00\x00\x02"

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

	nPack1, err := numPackObjects(pack1)
	if err != nil {
		return err
	}
	log.Printf("%s: %d objects", os.Args[1], nPack1)

	nPack2, err := numPackObjects(os.Stdin)
	if err != nil {
		return err
	}
	log.Printf("stdin: %d objects", nPack2)

	summer := sha1.New()
	stdout := io.MultiWriter(os.Stdout, summer)

	if _, err := fmt.Fprint(stdout, packMagic); err != nil {
		return err
	}

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, nPack1+nPack2)
	if _, err := stdout.Write(size); err != nil {
		return err
	}

	pack1Writer := &shaSplitter{w: stdout}
	if _, err := io.Copy(pack1Writer, pack1); err != nil {
		return err
	}

	pack2Writer := &shaSplitter{w: stdout}
	if _, err := io.Copy(pack2Writer, os.Stdin); err != nil {
		return err
	}

	if _, err := stdout.Write(summer.Sum(nil)); err != nil {
		return err
	}

	return nil
}

func numPackObjects(pack io.Reader) (uint32, error) {
	header := make([]byte, 12)
	if _, err := io.ReadFull(pack, header); err != nil {
		return 0, err
	}

	if magic := string(header[:len(packMagic)]); magic != packMagic {
		return 0, fmt.Errorf("bad pack header: %q", magic)
	}

	return binary.BigEndian.Uint32(header[len(packMagic):]), nil
}

const shaSize = 20

type shaSplitter struct {
	buf []byte
	w   io.Writer
}

func (sp *shaSplitter) Write(p []byte) (int, error) {
	sp.buf = append(sp.buf, p...)

	chunkBoundary := len(sp.buf) - shaSize
	if chunkBoundary <= 0 {
		return len(p), nil
	}

	if _, err := sp.w.Write(sp.buf[:chunkBoundary]); err != nil {
		return 0, err
	}

	copy(sp.buf, sp.buf[chunkBoundary:])
	sp.buf = sp.buf[:shaSize]

	return len(p), nil
}

func (sp *shaSplitter) Sha() ([]byte, error) {
	if n := len(sp.buf); n != shaSize {
		return nil, fmt.Errorf("error: %d bytes left in buffer", n)
	}

	return sp.buf, nil
}

type packReader struct {
	buf      []byte
	avail    []byte
	reader   io.Reader
	readErr  error
	sum      hash.Hash
	nObjects uint32
}

func NewPackReader(r io.Reader) (*packReader, error) {
	pr := &packReader{
		buf:    make([]byte, 4096),
		reader: r,
		sum:    sha1.New(),
	}

	header := make([]byte, 12)
	if _, err := io.ReadFull(pr.reader, header); err != nil {
		return nil, err
	}

	if magic := string(header[:len(packMagic)]); magic != packMagic {
		return nil, fmt.Errorf("bad pack header: %q", magic)
	}

	pr.nObjects = binary.BigEndian.Uint32(header[len(packMagic):])

	if _, err := pr.sum.Write(header); err != nil {
		return nil, err
	}

	return pr, nil
}

func (pr *packReader) Read(p []byte) (int, error) {
	if len(pr.avail) <= shaSize && pr.readErr == nil {
		copy(pr.buf, pr.avail)

		var nRead int
		nRead, pr.readErr = pr.reader.Read(pr.buf[len(pr.avail):])
		pr.avail = pr.buf[:len(pr.avail)+nRead]

		if nUncheckedBytes := len(pr.avail) - shaSize; nUncheckedBytes > 0 {
			if _, err := pr.sum.Write(pr.avail[:nUncheckedBytes]); err != nil {
				return 0, err
			}
		}

		if pr.readErr != nil && pr.readErr != io.EOF {
			return 0, pr.readErr
		}
	}

	if len(pr.avail) <= shaSize {
		if pr.readErr == io.EOF {
			if len(pr.avail) != shaSize {
				return 0, fmt.Errorf("short read: incomplete packfile checksum")
			}

			if !bytes.Equal(pr.sum.Sum(nil), pr.avail) {
				return 0, fmt.Errorf("packfile checksum mismatch")
			}
		}

		return 0, pr.readErr
	}

	nYielded := copy(p, pr.avail[:len(pr.avail)-shaSize])
	pr.avail = pr.avail[nYielded:]
	return nYielded, nil
}
