package diskcache

import (
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

// StreamCache allows a stream to be cached
type StreamCache struct {
	rootDir string
}

func NewStreamCache(rootDir string) *StreamCache {
	return &StreamCache{
		rootDir: rootDir,
	}
}

const logWithDiskCache = "disk-cache-entry"

// Get will attempt to retrieve the cached stream for the provided ID and tag.
// Each ID creates a namespace for tags. Invalidating an ID will invalidate all
// tags within it.
//
// If a cache miss occurs, the provided function will be invoked to obtain the
// stream and store it in the cache. Any error occurring in the provided
// function will be passed back to the caller unaltered.
func (sc StreamCache) Get(id, tag string, cacheMissFn func() (io.Reader, error)) (io.ReadCloser, error) {
	entryPath := filepath.Join(sc.rootDir, id, tag)

	if err := os.MkdirAll(filepath.Dir(entryPath), 0755); err != nil {
		return nil, err
	}

	f, err := os.Open(entryPath)
	switch {

	case err == nil:
		// cache hit
		return f, nil

	case os.IsNotExist(err):
		// cache miss
		break

	default:
		return nil, err

	}

	src, err := cacheMissFn()
	if err != nil {
		return nil, err
	}

	dst, err := os.Create(entryPath)
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		return nil, err
	}
	logrus.WithField(logWithDiskCache, entryPath).Infof("disk cache stored %d bytes", n)

	if _, err := dst.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}

	return dst, nil
}
