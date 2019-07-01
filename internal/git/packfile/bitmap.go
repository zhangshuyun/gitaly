package packfile

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/big"
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

	// TODO parse bitmap commits
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
	bits  int
	words int
	bm    *big.Int
}

const ewahTrailerLen = 4

func ReadEWAH(r io.Reader) (*EWAH, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	e := &EWAH{}

	uBits := binary.BigEndian.Uint32(header[:4])
	if uBits > math.MaxInt32 {
		return nil, fmt.Errorf("too many bits in bitmap: %d", uBits)
	}
	e.bits = int(uBits)

	uWords := binary.BigEndian.Uint32(header[4:])
	if uWords > math.MaxInt32 {
		return nil, fmt.Errorf("too many words in bitmap: %d", uWords)
	}
	e.words = int(uWords)

	rawSize := int64(e.words)*8 + ewahTrailerLen
	if rawSize > math.MaxInt32 {
		return nil, fmt.Errorf("EWAH bitmap does not fit in Go slice")
	}

	raw := make([]byte, int(rawSize))

	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, err
	}

	nUnpackedWords := e.bits / 64
	if e.bits%64 > 0 {
		nUnpackedWords++
	}

	buf := make([]byte, nUnpackedWords*8)
	bufPos := len(buf)

	for i := 0; i < e.words; {
		header := binary.BigEndian.Uint64(raw[8*i : 8*(i+1)])
		i++

		cleanBit := int(header & 1)
		nClean := uint32(header >> 1)
		nDirty := uint32(header >> 33)

		for ; nClean > 0; nClean-- {
			if cleanBit == 1 {
				copy(
					buf[bufPos-8:bufPos],
					[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
				)
			} else {
				// No need to copy zeros into buf
			}

			bufPos -= 8
		}

		for ; nDirty > 0; nDirty-- {
			copy(
				buf[bufPos-8:bufPos],
				raw[8*i:8*(i+1)],
			)
			bufPos -= 8
			i++
		}
	}

	e.bm = big.NewInt(0)
	e.bm.SetBytes(buf)

	return e, nil
}

func (e *EWAH) Scan(f func(int) error) error {
	for i := 0; i < e.bits; i++ {
		if e.bm.Bit(i) == 1 {
			if err := f(i); err != nil {
				return err
			}
		}
	}

	return nil
}
