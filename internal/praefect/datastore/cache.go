package datastore

import "sync"

// Cache is a thread-safe cache meant to be used to cache results from a datastore.
type Cache struct {
	sync.RWMutex
	d map[string]interface{}
}

// NewCache creates a new cache
func NewCache() *Cache {
	return &Cache{
		d: make(map[string]interface{}),
	}
}

// Bust creates a new map for cached values, effectively busting the entire cache.
func (c *Cache) Bust() {
	c.Lock()
	defer c.Unlock()

	c.d = make(map[string]interface{})
}

// GetFromCache either gets the results stored by key, or saves the results of a function call into its
// cache
func (c *Cache) GetFromCache(key string, f func() (interface{}, error)) (interface{}, error) {
	c.RLock()
	defer c.RUnlock()

	if res, ok := c.d[key]; ok {
		return res, nil
	}

	res, err := f()
	if err != nil {
		return nil, err
	}
	c.d[key] = res

	return res, nil
}
