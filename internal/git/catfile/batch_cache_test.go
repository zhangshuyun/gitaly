package catfile

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheAdd(t *testing.T) {
	const maxLen = 3
	bc := newCache(time.Hour, maxLen, defaultEvictionInterval)

	key0 := testKey(0)
	value0 := testValue()
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	key1 := testKey(1)
	bc.add(key1, testValue())
	requireCacheValid(t, bc)

	key2 := testKey(2)
	bc.add(key2, testValue())
	requireCacheValid(t, bc)

	// Because maxLen is 3, and key0 is oldest, we expect that adding key3
	// will kick out key0.
	key3 := testKey(3)
	bc.add(key3, testValue())
	requireCacheValid(t, bc)

	require.Equal(t, maxLen, bc.len(), "length should be maxLen")
	require.True(t, value0.isClosed(), "value0 should be closed")
	require.Equal(t, []key{key1, key2, key3}, keys(bc))
}

func TestCacheAddTwice(t *testing.T) {
	bc := newCache(time.Hour, 10, defaultEvictionInterval)

	key0 := testKey(0)
	value0 := testValue()
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	key1 := testKey(1)
	bc.add(key1, testValue())
	requireCacheValid(t, bc)

	require.Equal(t, key0, bc.head().key, "key0 should be oldest key")

	value2 := testValue()
	bc.add(key0, value2)
	requireCacheValid(t, bc)

	require.Equal(t, key1, bc.head().key, "key1 should be oldest key")
	require.Equal(t, value2, bc.head().value)

	require.True(t, value0.isClosed(), "value0 should be closed")
}

func TestCacheCheckout(t *testing.T) {
	bc := newCache(time.Hour, 10, defaultEvictionInterval)

	key0 := testKey(0)
	value0 := testValue()
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

func TestCacheEnforceTTL(t *testing.T) {
	ttl := time.Hour
	bc := newCache(ttl, 10, defaultEvictionInterval)

	sleep := func() { time.Sleep(2 * time.Millisecond) }

	key0 := testKey(0)
	value0 := testValue()
	bc.add(key0, value0)
	sleep()

	key1 := testKey(1)
	value1 := testValue()
	bc.add(key1, value1)
	sleep()

	cutoff := time.Now().Add(ttl)
	sleep()

	key2 := testKey(2)
	bc.add(key2, testValue())
	sleep()

	key3 := testKey(3)
	bc.add(key3, testValue())
	sleep()

	requireCacheValid(t, bc)

	// We expect this cutoff to cause eviction of key0 and key1 but no other keys.
	bc.enforceTTL(cutoff)

	requireCacheValid(t, bc)

	for i, v := range []*batch{value0, value1} {
		require.True(t, v.isClosed(), "value %d %v should be closed", i, v)
	}

	require.Equal(t, []key{key2, key3}, keys(bc), "remaining keys after EnforceTTL")

	bc.enforceTTL(cutoff)

	requireCacheValid(t, bc)
	require.Equal(t, []key{key2, key3}, keys(bc), "remaining keys after second EnforceTTL")
}

func TestAutoExpiry(t *testing.T) {
	ttl := 5 * time.Millisecond
	refresh := 1 * time.Millisecond
	bc := newCache(ttl, 10, refresh)

	key0 := testKey(0)
	value0 := testValue()
	bc.add(key0, value0)
	requireCacheValid(t, bc)

	require.Contains(t, keys(bc), key0, "key should still be in map")
	require.False(t, value0.isClosed(), "value should not have been closed")

	// Wait for the monitor goroutine to do its thing
	for i := 0; i < 100; i++ {
		if len(keys(bc)) == 0 {
			break
		}

		time.Sleep(refresh)
	}

	require.Empty(t, keys(bc), "key should no longer be in map")
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

func testValue() *batch { return &batch{} }

func testKey(i int) key { return key{sessionID: fmt.Sprintf("key-%d", i)} }

func keys(bc *BatchCache) []key {
	bc.entriesMutex.Lock()
	defer bc.entriesMutex.Unlock()

	var result []key
	for _, ent := range bc.entries {
		result = append(result, ent.key)
	}

	return result
}
