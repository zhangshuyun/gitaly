package datastore

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestDirectStorageProvider_GetSyncedNodes(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		for _, tc := range []struct {
			desc string
			ret  map[string]struct{}
			exp  []string
		}{
			{
				desc: "primary included",
				ret:  map[string]struct{}{"g2": {}, "g3": {}},
				exp:  []string{"g1", "g2", "g3"},
			},
			{
				desc: "distinct values",
				ret:  map[string]struct{}{"g1": {}, "g2": {}, "g3": {}},
				exp:  []string{"g1", "g2", "g3"},
			},
			{
				desc: "none",
				ret:  nil,
				exp:  []string{"g1"},
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				rs := &mockConsistentSecondariesProvider{}
				rs.On("GetConsistentSecondaries", ctx, "vs", "/repo/path", "g1").Return(tc.ret, nil)

				sp := NewDirectStorageProvider(rs)
				storages := sp.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
				require.ElementsMatch(t, tc.exp, storages)
			})
		}
	})

	t.Run("repository store returns an error", func(t *testing.T) {
		logger := testhelper.DiscardTestEntry(t)
		logHook := test.NewLocal(logger.Logger)

		ctx, cancel := testhelper.Context(testhelper.ContextWithLogger(logger))
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentSecondaries", ctx, "vs", "/repo/path", "g1").
			Return(nil, assert.AnError).
			Once()

		sp := NewDirectStorageProvider(rs)

		storages := sp.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1"}, storages)

		require.Len(t, logHook.AllEntries(), 1)
		require.Equal(t, "get consistent secondaries", logHook.LastEntry().Message)
		require.Equal(t, logrus.Fields{"error": assert.AnError}, logHook.LastEntry().Data)
		require.Equal(t, logrus.WarnLevel, logHook.LastEntry().Level)

		// "populate" metric is not set as there was an error and we don't want this result to be cached
		err := testutil.CollectAndCompare(sp, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_errors_total Total number of errors raised during defining up to date storages for reads distribution
			# TYPE gitaly_praefect_uptodate_storages_errors_total counter
			gitaly_praefect_uptodate_storages_errors_total{type="retrieve"} 1
		`))
		require.NoError(t, err)
	})
}

type mockConsistentSecondariesProvider struct {
	mock.Mock
}

func (m *mockConsistentSecondariesProvider) GetConsistentSecondaries(ctx context.Context, virtualStorage, relativePath, primary string) (map[string]struct{}, error) {
	args := m.Called(ctx, virtualStorage, relativePath, primary)
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
		rs.On("GetConsistentSecondaries", mock.Anything, "unknown", "/repo/path", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil).
			Once()

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		storages := cache.GetSyncedNodes(ctx, "unknown", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages)

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
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil).
			Once()

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// empty cache should be populated
		storages := cache.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
		`))
		require.NoError(t, err)

		// populated cache should return cached value
		storages = cache.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages)

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
		logger := testhelper.DiscardTestEntry(t)
		logHook := test.NewLocal(logger.Logger)

		ctx, cancel := testhelper.Context(testhelper.ContextWithLogger(logger))
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path", "g1").
			Return(nil, assert.AnError).
			Once()

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		storages := cache.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1"}, storages)

		require.Len(t, logHook.AllEntries(), 1)
		require.Equal(t, "get consistent secondaries", logHook.LastEntry().Message)
		require.Equal(t, logrus.Fields{"error": assert.AnError}, logHook.LastEntry().Data)
		require.Equal(t, logrus.WarnLevel, logHook.LastEntry().Level)

		// "populate" metric is not set as there was an error and we don't want this result to be cached
		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 1
			# HELP gitaly_praefect_uptodate_storages_errors_total Total number of errors raised during defining up to date storages for reads distribution
			# TYPE gitaly_praefect_uptodate_storages_errors_total counter
			gitaly_praefect_uptodate_storages_errors_total{type="retrieve"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("cache becomes disabled after handling invalid notification payload", func(t *testing.T) {
		logger := testhelper.DiscardTestEntry(t)
		logHook := test.NewLocal(logger.Logger)

		ctx, cancel := testhelper.Context(testhelper.ContextWithLogger(logger))
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/1", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil).
			Twice()

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		storages1 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages1)

		// invalid payload disables caching
		cache.Notification(glsql.Notification{Channel: "nt-channel", Payload: ``})

		logEntries := logHook.AllEntries()
		require.Len(t, logEntries, 1)
		assert.Equal(t, logrus.Fields{
			"component": "caching_storage_provider",
			"channel":   "nt-channel",
			"error":     io.EOF,
		}, logEntries[0].Data)
		assert.Equal(t, "received payload can't be processed", logEntries[0].Message)

		// second access omits cached data as caching should be disabled
		storages2 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages2)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 2
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 1
			# HELP gitaly_praefect_uptodate_storages_errors_total Total number of errors raised during defining up to date storages for reads distribution
			# TYPE gitaly_praefect_uptodate_storages_errors_total counter
			gitaly_praefect_uptodate_storages_errors_total{type="notification_decode"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("cache becomes enabled after handling valid payload after invalid payload", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/1", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil).
			Times(3)

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		storages1 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages1)

		// invalid payload disables caching
		cache.Notification(glsql.Notification{Payload: ``})

		// second access omits cached data as caching should be disabled
		storages2 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages2)

		// valid payload enables caching again
		cache.Notification(glsql.Notification{Payload: `{}`})

		// third access retrieves data and caches it
		storages3 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages3)

		// fourth access retrieves data from cache
		storages4 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages4)

		err = testutil.CollectAndCompare(cache, strings.NewReader(`
			# HELP gitaly_praefect_uptodate_storages_cache_access_total Total number of cache access operations during defining of up to date storages for reads distribution (per virtual storage)
			# TYPE gitaly_praefect_uptodate_storages_cache_access_total counter
			gitaly_praefect_uptodate_storages_cache_access_total{type="evict",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="hit",virtual_storage="vs"} 1
			gitaly_praefect_uptodate_storages_cache_access_total{type="miss",virtual_storage="vs"} 3
			gitaly_praefect_uptodate_storages_cache_access_total{type="populate",virtual_storage="vs"} 2
			# HELP gitaly_praefect_uptodate_storages_errors_total Total number of errors raised during defining up to date storages for reads distribution
			# TYPE gitaly_praefect_uptodate_storages_errors_total counter
			gitaly_praefect_uptodate_storages_errors_total{type="notification_decode"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("cache invalidation evicts cached entries", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		rs := &mockConsistentSecondariesProvider{}
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/1", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil)
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/2", "g1").
			Return(map[string]struct{}{"g2": {}}, nil)

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		path1Storages1 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, path1Storages1)
		path2Storages1 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/2", "g1")
		require.ElementsMatch(t, []string{"g1", "g2"}, path2Storages1)

		// notification evicts entries for '/repo/path/2' from the cache
		cache.Notification(glsql.Notification{Payload: `
			{
				"old":[
					{"virtual_storage": "bad", "relative_path": "/repo/path/1"}
				],
				"new":[{"virtual_storage": "vs", "relative_path": "/repo/path/2"}]
			}`},
		)

		// second access re-uses cached data for '/repo/path/1'
		path1Storages2 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, path1Storages2)
		// second access populates the cache again for '/repo/path/2'
		path2Storages2 := cache.GetSyncedNodes(ctx, "vs", "/repo/path/2", "g1")
		require.ElementsMatch(t, []string{"g1", "g2"}, path2Storages2)

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
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path", "g1").
			Return(map[string]struct{}{"g2": {}, "g3": {}}, nil)

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		// first access populates the cache
		storages1 := cache.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages1)

		// disconnection disables cache
		cache.Disconnect(assert.AnError)

		// second access retrieve data and doesn't populate the cache
		storages2 := cache.GetSyncedNodes(ctx, "vs", "/repo/path", "g1")
		require.ElementsMatch(t, []string{"g1", "g2", "g3"}, storages2)

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
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/1", "g1").Return(nil, nil)
		rs.On("GetConsistentSecondaries", mock.Anything, "vs", "/repo/path/2", "g1").Return(nil, nil)

		cache, err := NewCachingStorageProvider(ctxlogrus.Extract(ctx), rs, []string{"vs"})
		require.NoError(t, err)
		cache.Connected()

		nf1 := glsql.Notification{Payload: `{"new":[{"virtual_storage":"vs","relative_path":"/repo/path/1"}]}`}
		nf2 := glsql.Notification{Payload: `{"new":[{"virtual_storage":"vs","relative_path":"/repo/path/2"}]}`}

		var operations []func()
		for i := 0; i < 100; i++ {
			var f func()
			switch i % 6 {
			case 0, 1:
				f = func() { cache.GetSyncedNodes(ctx, "vs", "/repo/path/1", "g1") }
			case 2, 3:
				f = func() { cache.GetSyncedNodes(ctx, "vs", "/repo/path/2", "g1") }
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

	const dur = 50 * time.Millisecond

	var wg sync.WaitGroup
	begin := make(chan struct{})

	awaitKey := func(key string) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-begin

			defer sc.await(key)()
			time.Sleep(dur)
		}()
	}

	keys := []string{"a", "a", "b", "c", "d"}
	for _, key := range keys {
		awaitKey(key)
	}

	start := time.Now()
	close(begin)
	wg.Wait()
	duration := time.Since(start).Milliseconds()

	require.GreaterOrEqual(t, duration, 2*dur.Milliseconds(), "we use same key twice, so it should take at least 2 durations")
	require.Less(t, duration, int64(len(keys))*dur.Milliseconds(), "it should take less time as sequential processing")
}
