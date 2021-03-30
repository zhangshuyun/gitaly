package blob

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"golang.org/x/text/transform"
)

// lfsPointerFilter transforms and filters the output of `git cat-file --batch-check='%(objecttype)
// %(objectsize) %(objectname)' into a list of LFS pointer candidates. It strips all objects which
// are not blobs or whose size is too big for an LFS pointer.
type lfsPointerFilter struct{}

func (t lfsPointerFilter) Transform(dst, src []byte, atEOF bool) (int, int, error) {
	origDst, origSrc := dst, src

	for {
		if len(src) == 0 && atEOF {
			return 0, 0, nil
		}

		index := bytes.Index(src, []byte{'\n'})
		if index < 0 {
			if atEOF {
				return 0, 0, errors.New("invalid trailing LFS pointer line")
			}
			return len(origDst) - len(dst), len(origSrc) - len(src), transform.ErrShortSrc
		}

		objectInfo := bytes.SplitN(src[:index], []byte{' '}, 3)
		if len(objectInfo) != 3 {
			return 0, 0, fmt.Errorf("invalid LFS pointer candidate line %q", string(src[:index]))
		}
		objectSize, err := strconv.Atoi(string(objectInfo[1]))
		if err != nil {
			return 0, 0, fmt.Errorf("invalid LFS pointer size %q", string(objectInfo[1]))
		}

		if objectSize > lfsPointerMaxSize || !bytes.Equal(objectInfo[0], []byte("blob")) {
			src = src[index+1:]
			continue
		}

		oid := objectInfo[2]
		if len(dst) < len(oid)+1 {
			return len(origDst) - len(dst), len(origSrc) - len(src), transform.ErrShortDst
		}

		copy(dst, oid)
		dst[len(oid)] = '\n'

		src = src[index+1:]
		dst = dst[len(oid)+1:]
	}
}

func (t lfsPointerFilter) Reset() {}
