package datastore

import "sync"

type Cache struct {
	sync.RWMutex
	d        map[string]interface{}
}

func NewCache() *Cache {
	return &Cache{
		d:        make(map[string]interface{}),
	}
}

func (c *Cache) Bust() {
	c.Lock()
	defer c.Unlock()

	c.d = make(map[string]interface{})
}

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
