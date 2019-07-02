package packfile

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/internal/git/gitio"
)

const sumSize = sha1.Size

var (
	idxFileRegex = regexp.MustCompile(`\A(.*/pack-)([0-9a-f]{40})\.idx\z`)
)

type Index struct {
	ID                   string
	packBase             string
	packSize             int64
	Objects              []*Object
	ObjectsPackfileOrder []*Object
	fanOut               [256]int
	*Bitmap
}

func ReadIndex(idxPath string) (*Index, error) {
	reMatches := idxFileRegex.FindStringSubmatch(idxPath)
	if len(reMatches) == 0 {
		return nil, fmt.Errorf("invalid idx filename: %q", idxPath)
	}

	idx := &Index{
		packBase: reMatches[1] + reMatches[2],
		ID:       reMatches[2],
	}

	f, err := os.Open(idx.packBase + ".idx")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(gitio.NewHashfileReader(f))

	const sig = "\377tOc\x00\x00\x00\x02"
	actualSig, err := readN(r, len(sig))
	if s := string(actualSig); s != sig {
		return nil, fmt.Errorf("unexpected idx signature %q", s)
	}

	count, err := idx.nPackObjects()
	if err != nil {
		return nil, err
	}

	// TODO use a data structure other than a Go slice to hold the index
	// entries? Go slices use int as their index type, and int may not be
	// able to hold MaxUint32.
	if count > math.MaxInt32 {
		return nil, fmt.Errorf("too many objects in to fit in Go slice: %d", count)
	}
	idx.Objects = make([]*Object, count)

	for i := range idx.fanOut {
		n, err := readUint32(r)
		if err != nil {
			return nil, err
		}

		idx.fanOut[i] = int(n) // cast is safe because we know n<=len(idx.Objects)
	}

	buf := make([]byte, sumSize)
	for i := range idx.Objects {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		idx.Objects[i] = &Object{OID: hex.EncodeToString(buf)}
	}

	// Discard CRC32 values (one for each object)
	for range idx.Objects {
		if _, err := r.Discard(4); err != nil {
			return nil, err
		}
	}

	// Read 4-byte offsets
	const has8ByteOffsetMask = 1 << 31
	for _, obj := range idx.Objects {
		offset, err := readUint32(r)
		if err != nil {
			return nil, err
		}

		if offset&has8ByteOffsetMask == has8ByteOffsetMask {
			return nil, fmt.Errorf("handling 8-byte index offset is not supported")
		}

		obj.Offset = uint64(offset)
	}

	idxPackID, err := readN(r, sumSize)
	if err != nil {
		return nil, err
	}

	if s := hex.EncodeToString(idxPackID); s != idx.ID {
		return nil, fmt.Errorf("unexpected pack ID in idx: %s", s)
	}

	if _, err := r.Peek(1); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected trailing data, expected EOF")
		}
		return nil, err
	}

	return idx, nil
}

func (idx *Index) GetObject(oid string) (*Object, bool) {
	if len(oid) < 2 {
		return nil, false
	}

	radix64, err := strconv.ParseInt(oid[:2], 16, 0)
	if err != nil {
		return nil, false
	}

	radix := int(radix64)
	last := idx.fanOut[radix]
	first := 0
	if radix > 0 {
		first = idx.fanOut[radix-1]
	}

	searchRange := idx.Objects[first:last]
	objIdx := sort.Search(len(searchRange), func(i int) bool {
		return searchRange[i].OID >= oid
	})
	if objIdx == len(searchRange) {
		return nil, false
	}

	obj := searchRange[objIdx]
	if obj.OID != oid {
		return nil, false
	}

	return obj, true
}

func (idx *Index) nPackObjects() (uint32, error) {
	f, err := idx.openPack()
	if err != nil {
		return 0, err
	}
	defer f.Close()

	const sizeOffset = 8
	if _, err := f.Seek(sizeOffset, io.SeekStart); err != nil {
		return 0, err
	}

	return readUint32(f)
}

func (idx *Index) openPack() (f *os.File, err error) {
	packPath := idx.packBase + ".pack"
	f, err = os.Open(packPath)
	if err != nil {
		return nil, err
	}

	defer func(f *os.File) {
		if err != nil {
			f.Close()
		}
	}(f) // Bind f early so that we can do "return nil, err".

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	idx.packSize = fi.Size()

	const headerLen = 8
	header, err := readN(f, headerLen)
	if err != nil {
		return nil, err
	}

	const sig = "PACK\x00\x00\x00\x02"
	if s := string(header); s != sig {
		return nil, fmt.Errorf("unexpected pack signature %q", s)
	}

	if _, err := f.Seek(-sumSize, io.SeekEnd); err != nil {
		return nil, err
	}

	sum, err := readN(f, sumSize)
	if err != nil {
		return nil, err
	}

	if s := hex.EncodeToString(sum); s != idx.ID {
		return nil, fmt.Errorf("unexpected trailing checksum in .pack: %s", s)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}

func readUint32(r io.Reader) (uint32, error) {
	buf, err := readN(r, 4)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

func readUint64(r io.Reader) (uint64, error) {
	buf, err := readN(r, 8)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf), nil
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

const (
	digitSize = 16
	nBuckets  = 1 << digitSize
)

func (idx *Index) ComputePackfileOrder() {
	if len(idx.ObjectsPackfileOrder) > 0 {
		return
	}

	from := make([]*Object, len(idx.Objects))
	to := make([]*Object, len(idx.Objects))
	copy(from, idx.Objects)

	counts := make([]int, nBuckets)
	curDigits := make([]int, len(idx.Objects))

	for bits := uint(0); (idx.packSize >> bits) > 0; bits += digitSize {
		for i := range counts {
			counts[i] = 0
		}

		for j, obj := range from {
			curDigits[j] = computeDigit(obj.Offset, bits)
			counts[curDigits[j]]++
		}

		for i := 1; i < len(counts); i++ {
			counts[i] += counts[i-1]
		}

		for i := len(from) - 1; i >= 0; i-- {
			obj := from[i]
			to[counts[curDigits[i]]-1] = obj
			counts[curDigits[i]]--
		}

		to, from = from, to
	}

	idx.ObjectsPackfileOrder = from
}

func computeDigit(offset uint64, bits uint) int {
	return int((offset >> bits) & (nBuckets - 1))
}
