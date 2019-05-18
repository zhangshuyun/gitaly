package diskcache_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/diskcache"
)

func TestCacheDBGetStream(t *testing.T) {
	db, cleanup := tempDB(t)
	defer cleanup()

	namespace := "@hashed/abcd/1234"
	key := "InfoRefsUploadPack"

	_, err := db.GetStream(namespace, key)
	require.Error(t, diskcache.ErrNamespaceNotFound, err)

	// use a stream payload that is larger than the cache's internal buffer
	expectStr := strings.Repeat("1234567890", os.Getpagesize())

	err = db.PutStream(namespace, key, strings.NewReader(expectStr))
	require.NoError(t, err)

	stream, err := db.GetStream(namespace, key)
	require.NoError(t, err)

	actual, err := ioutil.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, expectStr, string(actual))
}

func tempDB(t testing.TB) (*diskcache.CacheDB, func()) {
	root, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	cleanup := func() { require.NoError(t, os.RemoveAll(root)) }

	dbPath := filepath.Join(root, "test.db")
	db, err := diskcache.CreateDB(dbPath)
	assert.NoError(t, err)

	return db, cleanup
}

func TestCacheDBDestroy(t *testing.T) {
	db, cleanup := tempDB(t)
	defer cleanup()

	require.NoError(t, db.Destroy())
}
