package packfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type EWAH struct {
	raw   []byte
	bits  uint32
	words uint32
}

func ReadEWAH(r io.Reader) (*EWAH, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	e := &EWAH{
		bits:  binary.BigEndian.Uint32(header[:4]),
		words: binary.BigEndian.Uint32(header[4:]),
	}

	rawSize := int64(e.words)*8 + 4
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
	lastByte := int(8 * e.words)
	for cursor := 0; cursor < lastByte; {
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

	return nil
}
