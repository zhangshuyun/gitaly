package diskcache_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/diskcache"
)

func TestStreamCache(t *testing.T) {
	root, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(root)) }()

	relPath := "@hashed/abcd1234/efgh5678" // unique to a repo

	cache := diskcache.NewStreamCache(root)

	expectBytes := []byte("this is a stream of bytes")
	srcCache := bytes.NewBuffer(expectBytes)

	cachedStream, err := cache.Get(relPath, "rpc-name", func() (io.Reader, error) {
		return srcCache, nil
	})
	require.NoError(t, err)

	cachedStreamBytes, err := ioutil.ReadAll(cachedStream)
	require.NoError(t, err)

	require.Equal(t, expectBytes, cachedStreamBytes)
}
