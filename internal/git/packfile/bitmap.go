package packfile

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/git/gitio"
)

type Bitmap struct {
	Commits        *EWAH
	Trees          *EWAH
	Blobs          *EWAH
	Tags           *EWAH
	nBitmapCommits uint32
}

func (idx *Index) LoadBitmap() error {
	f, err := os.Open(idx.packBase + ".bitmap")
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(gitio.NewHashfileReader(f))

	bmp := &Bitmap{}
	bmp.nBitmapCommits, err = idx.parseBitmapHeader(r)
	if err != nil {
		return err
	}

	for _, ptr := range []**EWAH{&bmp.Commits, &bmp.Trees, &bmp.Blobs, &bmp.Tags} {
		*ptr, err = ReadEWAH(r)
		if err != nil {
			return err
		}
	}
	for i := uint32(0); i < bmp.nBitmapCommits; i++ {
		const entryHeaderLen = 6
		if _, err := r.Discard(entryHeaderLen); err != nil {
			return err
		}

		if _, err := ReadEWAH(r); err != nil {
			return err
		}
	}

	// TODO handle hashcache

	if _, err := r.Peek(1); err != io.EOF {
		return fmt.Errorf("expected EOF, got %v", err)
	}

	idx.Bitmap = bmp
	return nil
}

func (idx *Index) parseBitmapHeader(r io.Reader) (uint32, error) {
	const headerLen = 32
	header, err := readN(r, headerLen)
	if err != nil {
		return 0, err
	}

	const sig = "BITM\x00\x01"
	if actualSig := string(header[:len(sig)]); actualSig != sig {
		return 0, fmt.Errorf("unexpected signature %q", actualSig)
	}
	header = header[len(sig):]

	const flagLen = 2
	flags := binary.BigEndian.Uint16(header[:flagLen])
	const minFlags = 1
	if flags&minFlags < minFlags {
		return 0, fmt.Errorf("invalid flags %x", flags)
	}
	header = header[flagLen:]

	count := binary.BigEndian.Uint32(header[:4])
	header = header[4:]

	if s := hex.EncodeToString(header); s != idx.ID {
		return 0, fmt.Errorf("unexpected pack ID in bitmap header: %s", s)
	}

	return count, nil
}

type EWAH struct {
	raw   []byte
	bits  uint32
	words uint32
}

const ewahTrailerLen = 4

func ReadEWAH(r io.Reader) (*EWAH, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	e := &EWAH{
		bits:  binary.BigEndian.Uint32(header[:4]),
		words: binary.BigEndian.Uint32(header[4:]),
	}

	rawSize := int64(e.words)*8 + ewahTrailerLen
	if rawSize > math.MaxInt32 {
		return nil, fmt.Errorf("EWAH bitmap does not fit in Go slice")
	}

	e.raw = make([]byte, int(rawSize))

	if _, err := io.ReadFull(r, e.raw); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *EWAH) Scan(f func(uint32) error) error {
	bit := uint32(0)
	cursor := 0
	lastByte := len(e.raw) - ewahTrailerLen
	for cursor < lastByte && bit < e.bits {
		header := binary.BigEndian.Uint64(e.raw[cursor : cursor+8])
		cursor += 8

		cleanBit := int(header & 1)
		nClean := uint32(header >> 1)
		nDirty := uint32(header >> 33)

		for i := uint32(0); i < nClean; i++ {
			for j := 0; j < 64; j++ {
				if cleanBit == 1 {
					if err := f(bit); err != nil {
						return err
					}
				}

				bit++
			}
		}

		for i := uint32(0); i < nDirty; i++ {
			word := binary.BigEndian.Uint64(e.raw[cursor : cursor+8])
			cursor += 8

			for j := uint(0); j < 64; j++ {
				if mask := uint64(1 << j); word&mask >= mask {
					if err := f(bit); err != nil {
						return err
					}
				}

				bit++
			}
		}
	}

	if cursor != lastByte {
		return fmt.Errorf("expected bitmap to use %d bytes, but consumed only %d", lastByte, cursor)
	}

	return nil
}
