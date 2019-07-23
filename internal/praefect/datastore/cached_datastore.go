package datastore

import (
	"fmt"
	"time"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/models"
)

type CacheOption func(*Cache)

type CachedDatastore struct {
	datastore ReplicasDatastore
	cache     *Cache
}

func NewCachedDatastore(datastore ReplicasDatastore, options ...CacheOption) *CachedDatastore {
	c := &CachedDatastore{
		datastore: datastore,
		cache:     NewCache(),
	}

	for _, option := range options {
		option(c.cache)
	}

	return c
}

func WithBustOnUpdate(l *pq.Listener) CacheOption {
	return func(c *Cache) {
		go waitForNotification(l, c)
	}
}

func waitForNotification(l *pq.Listener, c *Cache) {
	for {
		select {
		case <-l.Notify:
			fmt.Println("received an update event. busting the cache")
			c.Bust()
			fmt.Println("cache busted")
			return
		case <-time.After(90 * time.Second):
			fmt.Println("Received no events for 90 seconds, checking connection")
			go func() {
				l.Ping()
			}()
			return
		}
	}
}

func key(args ...interface{}) string {
	return fmt.Sprint(args...)
}

func (cd *CachedDatastore) GetSecondaries(relativePath string) ([]models.StorageNode, error) {
	res, err := cd.cache.GetFromCache(key("GetSecondaries", relativePath), func() (interface{}, error) {
		return cd.datastore.GetNodeStorages()
	})
	if err != nil {
		return nil, err
	}

	return res.([]models.StorageNode), nil
}

func (cd *CachedDatastore) GetNodesForStorage(storageName string) ([]models.StorageNode, error) {
	res, err := cd.cache.GetFromCache(key("GetNodesForStorage", storageName), func() (interface{}, error) {
		return cd.datastore.GetNodesForStorage(storageName)
	})
	if err != nil {
		return nil, err
	}

	return res.([]models.StorageNode), nil
}

func (cd *CachedDatastore) GetNodeStorages() ([]models.StorageNode, error) {
	res, err := cd.cache.GetFromCache(key("GetNodesStorages"), func() (interface{}, error) {
		return cd.datastore.GetNodeStorages()
	})
	if err != nil {
		return nil, err
	}

	return res.([]models.StorageNode), nil
}

func (cd *CachedDatastore) GetPrimary(relativePath string) (*models.StorageNode, error) {
	res, err := cd.cache.GetFromCache(key("GetPrimary", relativePath), func() (interface{}, error) {
		return cd.datastore.GetPrimary(relativePath)
	})
	if err != nil {
		return nil, err
	}

	return res.(*models.StorageNode), nil
}

func (cd *CachedDatastore) SetPrimary(relativePath string, storageNodeID int) error {
	return cd.datastore.SetPrimary(relativePath, storageNodeID)
}

func (cd *CachedDatastore) AddSecondary(relativePath string, storageNodeID int) error {
	return cd.datastore.AddSecondary(relativePath, storageNodeID)
}

func (cd *CachedDatastore) RemoveSecondary(relativePath string, storageNodeID int) error {
	return cd.datastore.RemoveSecondary(relativePath, storageNodeID)
}

func (cd *CachedDatastore) GetShard(relativePath string) (*models.Shard, error) {
	res, err := cd.cache.GetFromCache(key("GetShard", relativePath), func() (interface{}, error) {
		return cd.datastore.GetShard(relativePath)
	})
	if err != nil {
		return nil, err
	}

	return res.(*models.Shard), nil
}
