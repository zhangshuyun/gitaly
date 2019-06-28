package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"sort"

	"gitlab.com/gitlab-org/gitaly/internal/git/gitio"
	"gitlab.com/gitlab-org/gitaly/internal/git/packfile"
)

var (
	idxFileRegex = regexp.MustCompile(`\A(.*/pack-)([0-9a-f]{40})\.idx\z`)
	printMap     = flag.Bool("map", false, "output map visualization of packfile")
)

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		log.Fatal("usage: parse-bitmap [-map] PACK_IDX")
	}

	if err := _main(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}
}

func _main(packIdx string) error {
	reMatches := idxFileRegex.FindStringSubmatch(packIdx)
	if len(reMatches) == 0 {
		return fmt.Errorf("invalid idx filename: %q", packIdx)
	}

	packBase := reMatches[1] + reMatches[2]
	packID := reMatches[2]

	log.Print("parsing idx")
	idxObjects, err := readIndex(packBase, packID)
	if err != nil {
		return err
	}
	log.Printf("found %d objects", len(idxObjects))

	f, err := os.Open(packBase + ".bitmap")
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(gitio.NewHashfileReader(f))

	nBitmapCommits, err := parseBitmapHeader(r, packID)
	if err != nil {
		return err
	}

	// Sort objects by pack offset, because the object type bitmaps use
	// packfile order.
	packObjects := make([]*packfile.Object, len(idxObjects))
	copy(packObjects, idxObjects)

	log.Print("sorting object list")
	sort.Sort(packfile.PackfileOrder(packObjects))

	// The type bitmaps come in this specific order: commit, tree, blob, tag.
	log.Print("labeling objects")
	for _, t := range []packfile.ObjectType{packfile.TCommit, packfile.TTree, packfile.TBlob, packfile.TTag} {
		ewah, err := packfile.ReadEWAH(r)
		if err != nil {
			return err
		}

		setFunc := func(i uint32) error {
			obj := packObjects[i]
			if obj.Type != packfile.TUnknown {
				return fmt.Errorf("type already set for object %v", obj)
			}

			obj.Type = t

			return nil
		}

		if err := ewah.Scan(setFunc); err != nil {
			return err
		}
	}

	for _, obj := range packObjects {
		if obj.Type == packfile.TUnknown {
			return fmt.Errorf("object missing type label: %v", obj)
		}
	}

	for i := uint32(0); i < nBitmapCommits; i++ {
		const entryHeaderLen = 6
		if _, err := r.Discard(entryHeaderLen); err != nil {
			return err
		}

		if _, err := packfile.ReadEWAH(r); err != nil {
			return err
		}
	}

	if _, err := r.Peek(1); err != io.EOF {
		return fmt.Errorf("expected EOF, got %v", err)
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	fmt.Fprintf(out, "# pack-%s\n", packID)
	for _, o := range packObjects {
		if *printMap {
			c := ""
			switch o.Type {
			case packfile.TBlob:
				c = "b"
			case packfile.TCommit:
				c = "C"
			case packfile.TTree:
				c = "e"
			case packfile.TTag:
				c = "T"
			}

			fmt.Fprint(out, c+" ")
		} else {
			fmt.Fprintln(out, o)
		}
	}

	return nil
}

func parseBitmapHeader(r io.Reader, packID string) (uint32, error) {
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

	if s := hex.EncodeToString(header); s != packID {
		return 0, fmt.Errorf("unexpected pack ID in bitmap header: %s", s)
	}

	return count, nil
}

const sumSize = 20

func readIndex(packBase, packID string) ([]*packfile.Object, error) {
	f, err := os.Open(packBase + ".idx")
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

	count, err := nPackObjects(packBase, packID)
	if err != nil {
		return nil, err
	}

	// Fanout is used to speed up random access to the index. We are doing a
	// sequential read so we don't need the fanout table.
	const fanOutLen = 4 * 256
	if _, err := r.Discard(fanOutLen); err != nil {
		return nil, err
	}

	// TODO use a data structure other than a Go slice to hold the index
	// entries? Go slices use int as their index type, and int may not be
	// able to hold MaxUint32.
	if count > math.MaxInt32 {
		return nil, fmt.Errorf("too many objects in packfile to fit in Go slice: %d", count)
	}
	objects := make([]*packfile.Object, count)

	buf := make([]byte, sumSize)
	for i := 0; i < len(objects); i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		objects[i] = &packfile.Object{OID: hex.EncodeToString(buf)}
	}

	// Discard CRC32 values (one for each object)
	for i := 0; i < len(objects); i++ {
		if _, err := r.Discard(4); err != nil {
			return nil, err
		}
	}

	// Read 4-byte offsets
	has8ByteOffsets := false
	for i := 0; i < len(objects); i++ {
		offset, err := readUint32(r)
		if err != nil {
			return nil, err
		}

		const mask = 1 << 31
		if offset&mask == mask {
			has8ByteOffsets = true
			continue
		}

		objects[i].Offset = uint64(offset)
	}

	if has8ByteOffsets {
		for i := 0; i < len(objects); i++ {
			offset, err := readUint64(r)
			if err != nil {
				return nil, err
			}

			// TODO Not clear if all 8-byte offsets are populated, or only those that
			// don't fit into 4 bytes.
			if offset > 0 {
				objects[i].Offset = offset
			}
		}
	}

	idxPackID, err := readN(r, sumSize)
	if err != nil {
		return nil, err
	}

	if s := hex.EncodeToString(idxPackID); s != packID {
		return nil, fmt.Errorf("unexpected pack ID in idx: %s", s)
	}

	if _, err := r.Peek(1); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected trailing data, expected EOF")
		}
		return nil, err
	}

	return objects, nil
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func nPackObjects(packBase, packID string) (uint32, error) {
	f, err := openPack(packBase, packID)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	const headerLen = 12
	header, err := readN(f, headerLen)
	if err != nil {
		return 0, err
	}

	const sig = "PACK\x00\x00\x00\x02"
	if s := string(header[:len(sig)]); s != sig {
		return 0, fmt.Errorf("unexpected pack signature %q", s)
	}
	header = header[len(sig):]

	return binary.BigEndian.Uint32(header), nil
}

func openPack(packBase, packID string) (f *os.File, err error) {
	packPath := packBase + ".pack"
	f, err = os.Open(packPath)
	if err != nil {
		return nil, err
	}

	defer func(f *os.File) {
		if err != nil {
			f.Close()
		}
	}(f) // Bind f early so that we can do "return nil, err".

	if _, err := f.Seek(-sumSize, io.SeekEnd); err != nil {
		return nil, err
	}

	sum, err := readN(f, sumSize)
	if err != nil {
		return nil, err
	}

	if s := hex.EncodeToString(sum); s != packID {
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
