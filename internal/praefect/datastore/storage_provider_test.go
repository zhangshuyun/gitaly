package datastore

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type mockConsistentSecondariesProvider struct {
	mock.Mock
}

func (m *mockConsistentSecondariesProvider) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
	args := m.Called(ctx, virtualStorage, relativePath)
	val := args.Get(0)
	var res map[string]struct{}
	if val != nil {
		res = val.(map[string]struct{})
	}
	return res, args.Error(1)
}

func TestCachingStorageProvider_GetSyncedNodes(t *testing.T) {
	t.Run("unknown virtual storage", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "unknown", "/repo/path").
			Return(map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, nil).
			Once()

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		storages, err := cache.GetConsistentStorages(ctx, "unknown", "/repo/path")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="unknown"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("miss -> populate -> hit", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path").
			Return(map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, nil).
			Once()

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		storages, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)

		// populated cache should return cached value
		storages, err = cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="hit",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("repository store returns an error", func(t *testing.T) {
		ctx, cancel := testhelper.Context(testhelper.ContextWithLogger(testhelper.DiscardTestEntry(t)))
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path").
			Return(nil, assert.AnError).
			Once()

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		_, err = cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.Equal(t, assert.AnError, err)

		// "populate" metric is not set as there was an error and we don't want this result to be cached
		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("cache is disabled after handling invalid payload", func(t *testing.T) {
		logger := testhelper.DiscardTestEntry(t)
		logHook := test.NewLocal(logger.Logger)

		ctx, cancel := testhelper.Context(testhelper.ContextWithLogger(logger))
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path/1").
			Return(map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, nil).
			Times(4)

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages1)

		// invalid payload disables caching
		notification := glsql.Notification{Channel: "notification_channel_1", Payload: `_`}
		cache.Notification(notification)
		expErr := json.Unmarshal([]byte(notification.Payload), new(struct{}))

		// second access omits cached data as caching should be disabled
		storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages2)

		// third access retrieves data and caches it
		storages3, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages3)

		// fourth access retrieves data from cache
		storages4, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages4)

		require.Len(t, logHook.AllEntries(), 1)
		assert.Equal(t, "received payload can't be processed, cache disabled", logHook.LastEntry().Message)
		assert.Equal(t, logrus.Fields{
			"channel":   "notification_channel_1",
			"component": "caching_storage_provider",
			"error":     expErr,
		}, logHook.LastEntry().Data)
		assert.Equal(t, logrus.ErrorLevel, logHook.LastEntry().Level)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 4
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("cache invalidation evicts cached entries", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path/1").
			Return(map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, nil)
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path/2").
			Return(map[string]struct{}{"g1": {}, "g2": {}}, nil)

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		path1Storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, path1Storages1)
		path2Storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}}, path2Storages1)

		// notification evicts entries for '/repo/path/2' from the cache
		cache.Notification(glsql.Notification{Payload: `
			[
				{"virtual_storage": "bad", "relative_paths": ["/repo/path/1"]},
				{"virtual_storage": "vs", "relative_paths": ["/repo/path/2"]}
			]`},
		)

		// second access re-uses cached data for '/repo/path/1'
		path1Storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, path1Storages2)
		// second access populates the cache again for '/repo/path/2'
		path2Storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}}, path2Storages2)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="hit",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 3
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 3
		`))
		require.NoError(t, err)
	})

	t.Run("disconnect event disables cache", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path").
			Return(map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, nil)

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		storages1, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages1)

		// disconnection disables cache
		cache.Disconnect(assert.AnError)

		// second access retrieve data and doesn't populate the cache
		storages2, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path")
		require.NoError(t, err)
		require.Equal(t, map[string]struct{}{"g1": {}, "g2": {}, "g3": {}}, storages2)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 2
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("concurrent access", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path/1").Return(nil, nil)
		rs.On("GetConsistentStorages", mock.Anything, "vs", "/repo/path/2").Return(nil, nil)

		cache, err := NewCachingConsistentStoragesGetter(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		nf1 := glsql.Notification{Payload: `[{"virtual_storage": "vs", "relative_paths": ["/repo/path/1"]}]`}
		nf2 := glsql.Notification{Payload: `[{"virtual_storage": "vs", "relative_paths": ["/repo/path/2"]}]`}

		var operations []func()
		for i := 0; i < 100; i++ {
			var f func()
			switch i % 6 {
			case 0, 1:
				f = func() {
					_, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/1")
					assert.NoError(t, err)
				}
			case 2, 3:
				f = func() {
					_, err := cache.GetConsistentStorages(ctx, "vs", "/repo/path/2")
					assert.NoError(t, err)
				}
			case 4:
				f = func() { cache.Notification(nf1) }
			case 5:
				f = func() { cache.Notification(nf2) }
			}
			operations = append(operations, f)
		}

		var wg sync.WaitGroup
		wg.Add(len(operations))

		start := make(chan struct{})
		for _, operation := range operations {
			go func(operation func()) {
				defer wg.Done()
				<-start
				operation()
			}(operation)
		}

		close(start)
		wg.Wait()
	})
}

func TestSyncer_await(t *testing.T) {
	sc := syncer{inflight: map[string]chan struct{}{}}

	returned := make(chan string, 2)

	releaseA := sc.await("a")
	go func() {
		sc.await("a")()
		returned <- "waiter"
	}()

	// different key should proceed immediately
	sc.await("b")()

	// Yield to the 'waiter' goroutine. It should be blocked and
	// not send to the channel before releaseA is called.
	runtime.Gosched()

	returned <- "locker"
	releaseA()

	var returnOrder []string
	for i := 0; i < cap(returned); i++ {
		returnOrder = append(returnOrder, <-returned)
	}

	require.Equal(t, []string{"locker", "waiter"}, returnOrder)
}
