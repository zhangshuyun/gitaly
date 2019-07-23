package datastore_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
)

func TestCacheNoHits(t *testing.T) {
	c := datastore.NewCache()

	testCases := []interface{}{
		1, "string", struct{}{}, []string{"one", "two", "three"},
	}

	for i, value := range testCases {
		var cacheMiss bool
		res, err := c.GetFromCache(fmt.Sprintf("%d", i), func() (interface{}, error) {
			cacheMiss = true
			return value, nil
		})
		require.NoError(t, err)
		require.Equal(t, value, res)
		require.True(t, cacheMiss)
	}
}

func TestCacheAllHits(t *testing.T) {
	c := datastore.NewCache()

	key, value := "the key", "the value"

	// seed the cache
	_, err := c.GetFromCache(key, func() (interface{}, error) {
		return value, nil
	})
	require.NoError(t, err)

	testCases := []interface{}{
		1, "string", struct{}{}, []string{"one", "two", "three"},
	}

	for _, v := range testCases {
		var cacheMiss bool
		res, err := c.GetFromCache(key, func() (interface{}, error) {
			cacheMiss = true
			return v, nil
		})
		require.NoError(t, err)
		require.Equal(t, value, res, "we should retrieve the existing value in the cache")
		require.False(t, cacheMiss)
	}
}


func TestCacheMissAfterBust(t *testing.T) {
	c := datastore.NewCache()

	key, value := "the key", "the value"

	// seed the cache
	_, err := c.GetFromCache(key, func() (interface{}, error) {
		return value, nil
	})
	require.NoError(t, err)

	testCases := []interface{}{
		1, "string", struct{}{}, []string{"one", "two", "three"},
	}

	for _, v := range testCases {
		var cacheMiss bool
		res, err := c.GetFromCache(key, func() (interface{}, error) {
			cacheMiss = true
			return v, nil
		})
		require.NoError(t, err)
		require.Equal(t, value, res, "it should retrieve the existing value in the cache")
		require.False(t, cacheMiss)
	}

	c.Bust()

	newValue := []interface{}{"new", "value"}
	var cacheMiss bool

	res, err := c.GetFromCache(key, func() (interface{}, error) {
		cacheMiss = true
		return newValue, nil
	})
	require.NoError(t, err)
	require.Equal(t, newValue, res, "it should return the new value")
	require.True(t, cacheMiss)
}