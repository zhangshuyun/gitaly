package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// SecondariesProvider should provide information about secondary storages.
type SecondariesProvider interface {
	// GetConsistentSecondaries returns all secondaries with the same generation as the primary.
	GetConsistentSecondaries(ctx context.Context, virtualStorage, relativePath, primary string) (map[string]struct{}, error)
}

// DirectStorageProvider provides the latest state of the synced nodes.
type DirectStorageProvider struct {
	sp          SecondariesProvider
	errorsTotal *prometheus.CounterVec
}

// NewDirectStorageProvider returns a new storage provider.
func NewDirectStorageProvider(sp SecondariesProvider) *DirectStorageProvider {
	csp := &DirectStorageProvider{
		sp: sp,
		errorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_praefect_uptodate_storages_errors_total",
				Help: "Total number of errors raised during defining up to date storages for reads distribution",
			},
			[]string{"type"},
		),
	}

	return csp
}

func (c *DirectStorageProvider) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

func (c *DirectStorageProvider) Collect(collector chan<- prometheus.Metric) {
	c.errorsTotal.Collect(collector)
}

func (c *DirectStorageProvider) GetSyncedNodes(ctx context.Context, virtualStorage, relativePath, primaryStorage string) []string {
	storages, _ := c.getSyncedNodes(ctx, virtualStorage, relativePath, primaryStorage)
	return storages
}

func (c *DirectStorageProvider) getSyncedNodes(ctx context.Context, virtualStorage, relativePath, primaryStorage string) ([]string, bool) {
	upToDateStorages, err := c.sp.GetConsistentSecondaries(ctx, virtualStorage, relativePath, primaryStorage)
	if err != nil {
		c.errorsTotal.WithLabelValues("retrieve").Inc()
		// this is recoverable error - we can proceed with primary node
		ctxlogrus.Extract(ctx).WithError(err).Warn("get consistent secondaries")
		return []string{primaryStorage}, false
	}

	return combineStorages(upToDateStorages, primaryStorage), true
}

func combineStorages(upToDateStorages map[string]struct{}, primaryStorage string) []string {
	storages := make([]string, 0, len(upToDateStorages)+1)
	for upToDateStorage := range upToDateStorages {
		if upToDateStorage != primaryStorage {
			storages = append(storages, upToDateStorage)
		}
	}

	return append(storages, primaryStorage)
}

// errNotExistingVirtualStorage indicates that the requested virtual storage can't be found or not configured.
var errNotExistingVirtualStorage = errors.New("virtual storage does not exist")

// CachingStorageProvider is a storage provider that caches up to date storages by repository.
// Each virtual storage has it's own cache that invalidates entries based on notifications.
type CachingStorageProvider struct {
	*DirectStorageProvider
	// caches is per virtual storage cache. It is initialized once on construction.
	caches map[string]*lru.Cache
	// access is access method to use: 0 - without caching; 1 - with caching.
	access int32
	// syncer allows to sync retrieval operations to omit unnecessary runs.
	syncer syncer
	// callbackLogger should be used only inside of the methods used as callbacks.
	callbackLogger   logrus.FieldLogger
	cacheAccessTotal *prometheus.CounterVec
}

// NewCachingStorageProvider returns a storage provider that uses caching.
func NewCachingStorageProvider(logger logrus.FieldLogger, sp SecondariesProvider, virtualStorages []string) (*CachingStorageProvider, error) {
	csp := &CachingStorageProvider{
		DirectStorageProvider: NewDirectStorageProvider(sp),
		caches:                make(map[string]*lru.Cache, len(virtualStorages)),
		syncer:                syncer{inflight: map[string]chan struct{}{}},
		callbackLogger:        logger.WithField("component", "caching_storage_provider"),
		cacheAccessTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_praefect_uptodate_storages_cache_access_total",
				Help: "Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)",
			},
			[]string{"virtual_storage", "type"},
		),
	}

	for _, virtualStorage := range virtualStorages {
		virtualStorage := virtualStorage
		cache, err := lru.NewWithEvict(2<<20, func(key interface{}, value interface{}) {
			csp.cacheAccessTotal.WithLabelValues(virtualStorage, "evict").Inc()
		})
		if err != nil {
			return nil, err
		}
		csp.caches[virtualStorage] = cache
	}

	return csp, nil
}

type (
	notificationEntry struct {
		VirtualStorage string `json:"virtual_storage"`
		RelativePath   string `json:"relative_path"`
	}

	changeNotification struct {
		Old []notificationEntry `json:"old"`
		New []notificationEntry `json:"new"`
	}
)

func (c *CachingStorageProvider) Notification(n glsql.Notification) {
	var change changeNotification
	if err := json.NewDecoder(strings.NewReader(n.Payload)).Decode(&change); err != nil {
		c.disableCaching() // as we can't update cache properly we should disable it
		c.errorsTotal.WithLabelValues("notification_decode").Inc()
		c.callbackLogger.WithError(err).WithField("channel", n.Channel).Error("received payload can't be processed")
		return
	}

	c.enableCaching()

	entries := map[string][]string{}
	for _, notificationEntries := range [][]notificationEntry{change.Old, change.New} {
		for _, entry := range notificationEntries {
			entries[entry.VirtualStorage] = append(entries[entry.VirtualStorage], entry.RelativePath)
		}
	}

	for virtualStorage, relativePaths := range entries {
		cache, found := c.getCache(virtualStorage)
		if !found {
			c.callbackLogger.WithError(errNotExistingVirtualStorage).WithField("virtual_storage", virtualStorage).Error("cache not found")
			continue
		}

		for _, relativePath := range relativePaths {
			cache.Remove(relativePath)
		}
	}
}

func (c *CachingStorageProvider) Connected() {
	c.enableCaching() // (re-)enable cache usage
}

func (c *CachingStorageProvider) Disconnect(error) {
	// disable cache usage as it could be outdated
	c.disableCaching()
}

func (c *CachingStorageProvider) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

func (c *CachingStorageProvider) Collect(collector chan<- prometheus.Metric) {
	c.errorsTotal.Collect(collector)
	c.cacheAccessTotal.Collect(collector)
}

func (c *CachingStorageProvider) enableCaching() {
	atomic.StoreInt32(&c.access, 1)
}

func (c *CachingStorageProvider) disableCaching() {
	atomic.StoreInt32(&c.access, 0)

	for _, cache := range c.caches {
		cache.Purge()
	}
}

func (c *CachingStorageProvider) getCache(virtualStorage string) (*lru.Cache, bool) {
	val, found := c.caches[virtualStorage]
	return val, found
}

func (c *CachingStorageProvider) cacheMiss(ctx context.Context, virtualStorage, relativePath, primaryStorage string) ([]string, bool) {
	c.cacheAccessTotal.WithLabelValues(virtualStorage, "miss").Inc()
	return c.getSyncedNodes(ctx, virtualStorage, relativePath, primaryStorage)
}

func (c *CachingStorageProvider) tryCache(ctx context.Context, virtualStorage, relativePath, primaryStorage string) (func(), *lru.Cache, []string, bool) {
	populateDone := func() {} // should be called AFTER any cache population is done

	cache, found := c.getCache(virtualStorage)
	if !found {
		return populateDone, nil, nil, false
	}

	if storages, found := getStringSlice(cache, relativePath); found {
		return populateDone, cache, storages, true
	}

	// synchronises concurrent attempts to update cache for the same key.
	populateDone = c.syncer.await(relativePath)

	if storages, found := getStringSlice(cache, relativePath); found {
		return populateDone, cache, storages, true
	}

	return populateDone, cache, nil, false
}

func (c *CachingStorageProvider) isCacheEnabled() bool { return atomic.LoadInt32(&c.access) != 0 }

func (c *CachingStorageProvider) GetSyncedNodes(ctx context.Context, virtualStorage, relativePath, primaryStorage string) []string {
	var cache *lru.Cache

	if c.isCacheEnabled() {
		var storages []string
		var ok bool
		var populationDone func()

		populationDone, cache, storages, ok = c.tryCache(ctx, virtualStorage, relativePath, primaryStorage)
		defer populationDone()
		if ok {
			c.cacheAccessTotal.WithLabelValues(virtualStorage, "hit").Inc()
			return storages
		}
	}

	storages, ok := c.cacheMiss(ctx, virtualStorage, relativePath, primaryStorage)
	if ok && cache != nil {
		cache.Add(relativePath, storages)
		c.cacheAccessTotal.WithLabelValues(virtualStorage, "populate").Inc()
	}
	return storages
}

func getStringSlice(cache *lru.Cache, key string) ([]string, bool) {
	val, found := cache.Get(key)
	vals, _ := val.([]string)
	return vals, found
}

// syncer allows to sync access to a particular key.
type syncer struct {
	// inflight contains set of keys already acquired for sync.
	inflight map[string]chan struct{}
	mtx      sync.Mutex
}

// await acquires lock for provided key and returns a callback to invoke once the key could be released.
// If key is already acquired the call will be blocked until callback for that key won't be called.
func (sc *syncer) await(key string) func() {
	sc.mtx.Lock()

	if cond, found := sc.inflight[key]; found {
		sc.mtx.Unlock()

		<-cond // the key is acquired, wait until it is released

		return func() {}
	}

	defer sc.mtx.Unlock()

	cond := make(chan struct{})
	sc.inflight[key] = cond

	return func() {
		sc.mtx.Lock()
		defer sc.mtx.Unlock()

		delete(sc.inflight, key)

		close(cond)
	}
}
