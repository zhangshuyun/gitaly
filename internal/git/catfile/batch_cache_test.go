package catfile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestCache_add(t *testing.T) {
	const maxLen = 3
	bc := newCache(time.Hour, maxLen, defaultEvictionInterval)

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := newCacheKey("0", repo)
	value0 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	key1 := newCacheKey("1", repo)
	bc.add(key1, mustCreateBatch(t, cfg, bc, repo))
	requireCacheValid(t, bc)

	key2 := newCacheKey("2", repo)
	bc.add(key2, mustCreateBatch(t, cfg, bc, repo))
	requireCacheValid(t, bc)

	// Because maxLen is 3, and key0 is oldest, we expect that adding key3
	// will kick out key0.
	key3 := newCacheKey("3", repo)
	bc.add(key3, mustCreateBatch(t, cfg, bc, repo))
	requireCacheValid(t, bc)

	require.Equal(t, maxLen, bc.len(), "length should be maxLen")
	require.True(t, value0.isClosed(), "value0 should be closed")
	require.Equal(t, []key{key1, key2, key3}, keys(t, bc))
}

func TestCache_addTwice(t *testing.T) {
	bc := newCache(time.Hour, 10, defaultEvictionInterval)

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := newCacheKey("0", repo)
	value0 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	key1 := newCacheKey("1", repo)
	value1 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key1, value1)
	requireCacheValid(t, bc)

	require.Equal(t, key0, bc.head().key, "key0 should be oldest key")

	value2 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value2)
	requireCacheValid(t, bc)

	require.Equal(t, key1, bc.head().key, "key1 should be oldest key")
	require.Equal(t, value1, bc.head().value)

	require.True(t, value0.isClosed(), "value0 should be closed")
}

func TestCache_checkout(t *testing.T) {
	bc := newCache(time.Hour, 10, defaultEvictionInterval)

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := newCacheKey("0", repo)
	value0 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value0)

	v, ok := bc.checkout(key{sessionID: "foo"})
	requireCacheValid(t, bc)
	require.Nil(t, v, "expect nil value when key not found")
	require.False(t, ok, "ok flag")

	v, ok = bc.checkout(key0)
	requireCacheValid(t, bc)

	require.Equal(t, value0, v)
	require.True(t, ok, "ok flag")

	require.False(t, v.isClosed(), "value should not be closed after checkout")

	v, ok = bc.checkout(key0)
	require.False(t, ok, "ok flag after second checkout")
	require.Nil(t, v, "value from second checkout")
}

func TestCache_enforceTTL(t *testing.T) {
	ttl := time.Hour
	bc := newCache(ttl, 10, defaultEvictionInterval)

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	sleep := func() { time.Sleep(2 * time.Millisecond) }

	key0 := newCacheKey("0", repo)
	value0 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value0)
	sleep()

	key1 := newCacheKey("1", repo)
	value1 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key1, value1)
	sleep()

	cutoff := time.Now().Add(ttl)
	sleep()

	key2 := newCacheKey("2", repo)
	bc.add(key2, mustCreateBatch(t, cfg, bc, repo))
	sleep()

	key3 := newCacheKey("3", repo)
	bc.add(key3, mustCreateBatch(t, cfg, bc, repo))
	sleep()

	requireCacheValid(t, bc)

	// We expect this cutoff to cause eviction of key0 and key1 but no other keys.
	bc.enforceTTL(cutoff)

	requireCacheValid(t, bc)

	for i, v := range []*batch{value0, value1} {
		require.True(t, v.isClosed(), "value %d %v should be closed", i, v)
	}

	require.Equal(t, []key{key2, key3}, keys(t, bc), "remaining keys after EnforceTTL")

	bc.enforceTTL(cutoff)

	requireCacheValid(t, bc)
	require.Equal(t, []key{key2, key3}, keys(t, bc), "remaining keys after second EnforceTTL")
}

func TestCache_autoExpiry(t *testing.T) {
	ttl := 5 * time.Millisecond
	refresh := 1 * time.Millisecond
	bc := newCache(ttl, 10, refresh)

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := newCacheKey("0", repo)
	value0 := mustCreateBatch(t, cfg, bc, repo)
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	require.Contains(t, keys(t, bc), key0, "key should still be in map")
	require.False(t, value0.isClosed(), "value should not have been closed")

	// Wait for the monitor goroutine to do its thing
	for i := 0; i < 100; i++ {
		if len(keys(t, bc)) == 0 {
			break
		}

		time.Sleep(refresh)
	}

	require.Empty(t, keys(t, bc), "key should no longer be in map")
	require.True(t, value0.isClosed(), "value should be closed after eviction")
}

func requireCacheValid(t *testing.T, bc *BatchCache) {
	bc.entriesMutex.Lock()
	defer bc.entriesMutex.Unlock()

	for _, ent := range bc.entries {
		v := ent.value
		require.False(t, v.isClosed(), "values in cache should not be closed: %v %v", ent, v)
	}
}

func mustCreateBatch(t *testing.T, cfg config.Cfg, bc *BatchCache, repo repository.GitRepo) *batch {
	t.Helper()

	ctx, cancel := testhelper.Context()
	t.Cleanup(cancel)

	batch, _, err := bc.newBatch(ctx, newRepoExecutor(t, cfg, repo))
	require.NoError(t, err)

	return batch
}

func keys(t *testing.T, bc *BatchCache) []key {
	t.Helper()

	bc.entriesMutex.Lock()
	defer bc.entriesMutex.Unlock()

	var result []key
	for _, ent := range bc.entries {
		result = append(result, ent.key)
	}

	return result
}
