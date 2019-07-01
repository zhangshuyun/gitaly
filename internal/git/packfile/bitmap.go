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
	Commits       *EWAH
	Trees         *EWAH
	Blobs         *EWAH
	Tags          *EWAH
	BitmapCommits []*BitmapCommit
	flags         int
}

type BitmapCommit struct {
	OID string
	*EWAH
	xorOffset byte
	flags     byte
}

func (idx *Index) LoadBitmap() error {
	f, err := os.Open(idx.packBase + ".bitmap")
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(gitio.NewHashfileReader(f))

	bmp := &Bitmap{}
	if err := bmp.parseBitmapHeader(r, idx); err != nil {
		return err
	}

	for _, ptr := range []**EWAH{&bmp.Commits, &bmp.Trees, &bmp.Blobs, &bmp.Tags} {
		*ptr, err = ReadEWAH(r)
		if err != nil {
			return err
		}

		if err := (*ptr).Parse(); err != nil {
			return err
		}
	}

	for i := range bmp.BitmapCommits {
		header, err := readN(r, 6)
		if err != nil {
			return err
		}

		bc := &BitmapCommit{
			OID:       idx.Objects[binary.BigEndian.Uint32(header[:4])].OID,
			xorOffset: header[4],
			flags:     header[5],
		}

		if bc.EWAH, err = ReadEWAH(r); err != nil {
			return err
		}

		bmp.BitmapCommits[i] = bc
	}

	if bmp.flags&BITMAP_OPT_HASH_CACHE > 0 {
		// Discard bitmap hash cache
		for range idx.Objects {
			if _, err := r.Discard(4); err != nil {
				return err
			}
		}
	}

	if _, err := r.Peek(1); err != io.EOF {
		return fmt.Errorf("expected EOF, got %v", err)
	}

	idx.Bitmap = bmp
	return nil
}

const (
	BITMAP_OPT_FULL_DAG   = 1
	BITMAP_OPT_HASH_CACHE = 4
)

func (bmp *Bitmap) parseBitmapHeader(r io.Reader, idx *Index) error {
	const headerLen = 32
	header, err := readN(r, headerLen)
	if err != nil {
		return err
	}

	const sig = "BITM\x00\x01"
	if actualSig := string(header[:len(sig)]); actualSig != sig {
		return fmt.Errorf("unexpected signature %q", actualSig)
	}
	header = header[len(sig):]

	const flagLen = 2
	bmp.flags = int(binary.BigEndian.Uint16(header[:flagLen]))
	header = header[flagLen:]

	const knownFlags = BITMAP_OPT_FULL_DAG | BITMAP_OPT_HASH_CACHE
	if bmp.flags&^knownFlags != 0 || (bmp.flags&BITMAP_OPT_FULL_DAG == 0) {
		return fmt.Errorf("invalid flags %x", bmp.flags)
	}

	const countLen = 4
	count := binary.BigEndian.Uint32(header[:countLen])
	header = header[countLen:]
	bmp.BitmapCommits = make([]*BitmapCommit, count)

	if s := hex.EncodeToString(header); s != idx.ID {
		return fmt.Errorf("unexpected pack ID in bitmap header: %s", s)
	}

	return nil
}

type EWAH struct {
	bits  int
	words int
	raw   []byte
	bm    *big.Int
}

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

	const ewahTrailerLen = 4
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

func (e *EWAH) Parse() error {
	nUnpackedWords := e.bits / 64
	if e.bits%64 > 0 {
		nUnpackedWords++
	}

	buf := make([]byte, nUnpackedWords*8)
	bufPos := len(buf)

	for i := 0; i < e.words; {
		header := binary.BigEndian.Uint64(e.raw[8*i : 8*(i+1)])
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
				e.raw[8*i:8*(i+1)],
			)
			bufPos -= 8
			i++
		}
	}

	e.bm = big.NewInt(0)
	e.bm.SetBytes(buf)

	return nil
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
