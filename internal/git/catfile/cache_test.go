package catfile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

func TestStack_add(t *testing.T) {
	const maxLen = 3
	s := &stack{maxLen: maxLen}

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key0, value0, time.Hour, cancel)
	requireStackValid(t, s)

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key1, value1, time.Hour, cancel)
	requireStackValid(t, s)

	key2 := mustCreateKey(t, "2", repo)
	value2, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key2, value2, time.Hour, cancel)
	requireStackValid(t, s)

	// Because maxLen is 3, and key0 is oldest, we expect that adding key3
	// will kick out key0.
	key3 := mustCreateKey(t, "3", repo)
	value3, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key3, value3, time.Hour, cancel)
	requireStackValid(t, s)

	require.Equal(t, maxLen, s.EntryCount(), "length should be maxLen")
	require.True(t, value0.isClosed(), "value0 should be closed")
	require.Equal(t, []key{key1, key2, key3}, keys(t, s))
}

func TestStack_addTwice(t *testing.T) {
	s := &stack{maxLen: 10}

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key0, value0, time.Hour, cancel)
	requireStackValid(t, s)

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key1, value1, time.Hour, cancel)
	requireStackValid(t, s)

	require.Equal(t, key0, s.head().key, "key0 should be oldest key")

	value2, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key0, value2, time.Hour, cancel)
	requireStackValid(t, s)

	require.Equal(t, key1, s.head().key, "key1 should be oldest key")
	require.Equal(t, value1, s.head().value)

	require.True(t, value0.isClosed(), "value0 should be closed")
}

func TestStack_Checkout(t *testing.T) {
	s := &stack{maxLen: 10}

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key0, value0, time.Hour, cancel)

	entry, ok := s.Checkout(key{sessionID: "foo"})
	requireStackValid(t, s)
	require.Nil(t, entry, "expect nil value when key not found")
	require.False(t, ok, "ok flag")

	entry, ok = s.Checkout(key0)
	requireStackValid(t, s)

	require.Equal(t, value0, entry.value)
	require.True(t, ok, "ok flag")

	require.False(t, entry.value.isClosed(), "value should not be closed after checkout")

	entry, ok = s.Checkout(key0)
	require.False(t, ok, "ok flag after second checkout")
	require.Nil(t, entry, "value from second checkout")
}

func TestStack_EnforceTTL(t *testing.T) {
	ttl := time.Hour
	s := &stack{maxLen: 10}

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	sleep := func() { time.Sleep(2 * time.Millisecond) }

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key0, value0, ttl, cancel)
	sleep()

	key1 := mustCreateKey(t, "1", repo)
	value1, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key1, value1, ttl, cancel)
	sleep()

	cutoff := time.Now().Add(ttl)
	sleep()

	key2 := mustCreateKey(t, "2", repo)
	value2, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key2, value2, ttl, cancel)
	sleep()

	key3 := mustCreateKey(t, "3", repo)
	value3, cancel := mustCreateCacheable(t, cfg, repo)
	s.Add(key3, value3, ttl, cancel)
	sleep()

	requireStackValid(t, s)

	// We expect this cutoff to cause eviction of key0 and key1 but no other keys.
	s.EnforceTTL(cutoff)

	requireStackValid(t, s)

	for i, v := range []cacheable{value0, value1} {
		require.True(t, v.isClosed(), "value %d %v should be closed", i, v)
	}

	require.Equal(t, []key{key2, key3}, keys(t, s), "remaining keys after EnforceTTL")

	s.EnforceTTL(cutoff)

	requireStackValid(t, s)
	require.Equal(t, []key{key2, key3}, keys(t, s), "remaining keys after second EnforceTTL")
}

func TestCache_autoExpiry(t *testing.T) {
	ttl := 5 * time.Millisecond
	refresh := 1 * time.Millisecond
	c := newCache(ttl, 10, refresh)
	defer c.Stop()

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	key0 := mustCreateKey(t, "0", repo)
	value0, cancel := mustCreateCacheable(t, cfg, repo)
	c.objectReaders.Add(key0, value0, ttl, cancel)
	requireStackValid(t, &c.objectReaders)

	require.Contains(t, keys(t, &c.objectReaders), key0, "key should still be in map")
	require.False(t, value0.isClosed(), "value should not have been closed")

	// Wait for the monitor goroutine to do its thing
	for i := 0; i < 100; i++ {
		if len(keys(t, &c.objectReaders)) == 0 {
			break
		}

		time.Sleep(refresh)
	}

	require.Empty(t, keys(t, &c.objectReaders), "key should no longer be in map")
	require.True(t, value0.isClosed(), "value should be closed after eviction")
}

func TestCache_BatchProcess(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(time.Hour, 10, time.Hour)
	defer cache.Stop()
	cache.cachedProcessDone = sync.NewCond(&sync.Mutex{})

	t.Run("uncancellable", func(t *testing.T) {
		ctx := context.Background()

		require.PanicsWithValue(t, "empty ctx.Done() in catfile.Batch.New()", func() {
			_, _ = cache.BatchProcess(ctx, repoExecutor)
		})
	})

	t.Run("uncacheable", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = correlation.ContextWithCorrelation(ctx, "1")

		// The context doesn't carry a session ID and is thus uncacheable.
		// The process should never get returned to the cache and must be
		// killed on context cancellation.
		batchProcess, err := cache.BatchProcess(ctx, repoExecutor)
		require.NoError(t, err)

		batchProcessImpl, ok := batchProcess.(*batch)
		require.True(t, ok, "expected batch")

		objectReaderImpl, ok := batchProcessImpl.objectReader.(*objectReader)
		require.True(t, ok, "expected objectReader")

		objectInfoReaderImpl, ok := batchProcessImpl.objectInfoReader.(*objectInfoReader)
		require.True(t, ok, "expected objectInfoReader")

		correlation := correlation.ExtractFromContext(objectReaderImpl.creationCtx)
		require.Equal(t, "1", correlation)

		cancel()

		// We're cheating a bit here to avoid creating a racy test by reaching into the
		// batch processes and trying to read from their stdout. If the cancel did kill the
		// process as expected, then the stdout should be closed and we'll get an EOF.
		for _, reader := range []io.Reader{objectInfoReaderImpl.cmd, objectReaderImpl.cmd} {
			output, err := io.ReadAll(reader)
			if err != nil {
				require.True(t, errors.Is(err, os.ErrClosed))
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, output)
		}

		require.True(t, objectReaderImpl.isClosed())
		require.True(t, objectInfoReaderImpl.isClosed())

		require.Empty(t, keys(t, &cache.objectReaders))
	})

	t.Run("cacheable", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		batchProcess, err := cache.BatchProcess(ctx, repoExecutor)
		require.NoError(t, err)

		batchProcessImpl, ok := batchProcess.(*batch)
		require.True(t, ok, "expected batch")

		objectReaderImpl, ok := batchProcessImpl.objectReader.(*objectReader)
		require.True(t, ok, "expected objectReader")

		// The correlation ID must be empty given that this will be a cached long-running
		// processes that can be reused across multpile RPC calls.
		correlation := correlation.ExtractFromContext(objectReaderImpl.creationCtx)
		require.Empty(t, correlation)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		for {
			cache.cachedProcessDone.Wait()
			// We cannot exactly tell whether we're going to see both signals or only
			// one given that it depends on the order in which the mutex is acquired. We
			// thus use cache lengths as indicator that both processes have been
			// returned.
			if len(keys(t, &cache.objectReaders))+len(keys(t, &cache.objectInfoReaders)) == 2 {
				break
			}
		}
		cache.cachedProcessDone.L.Unlock()

		for _, stack := range []*stack{&cache.objectReaders, &cache.objectInfoReaders} {
			require.Equal(t, []key{{
				sessionID:   "1",
				repoStorage: repo.GetStorageName(),
				repoRelPath: repo.GetRelativePath(),
			}}, keys(t, stack))
		}

		// Assert that we can still read from the cached process.
		_, err = batchProcess.Info(ctx, "refs/heads/master")
		require.NoError(t, err)
	})

	t.Run("dirty process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		batchProcess, err := cache.BatchProcess(ctx, repoExecutor)
		require.NoError(t, err)

		// Write a 16MB blob into the repository which we can read. We want to have a blob
		// which is big enough so it doesn't get fully written into the buffered output.
		// Otherwise, reading the blob below may succeed even though we expect the stdout to
		// be closed already.
		blobID := gittest.WriteBlob(t, cfg, repoPath, bytes.Repeat([]byte{0}, 1024*1024*16))

		// While we request object data, we do not consume it at all. The reader is thus
		// dirty and cannot be reused and shouldn't be returned to the cache.
		object, err := batchProcess.Blob(ctx, blobID.Revision())
		require.NoError(t, err)

		dirtyCount := testutil.ToFloat64(cache.catfileCacheCounter.WithLabelValues("dirty"))

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it. The code is a bit more involved in
		// order to avoid a race: we listen on the process-done condition until we have
		// another dirty process. This needs to be done because we can see two process-done
		// signals, and we cannot decide whether we will see both or only one. We thus use
		// the dirty-counter as heuristic.
		cache.cachedProcessDone.L.Lock()
		cancel()
		for {
			cache.cachedProcessDone.Wait()
			newDirtyCount := testutil.ToFloat64(cache.catfileCacheCounter.WithLabelValues("dirty"))
			if dirtyCount+1 == newDirtyCount {
				break
			}
		}
		cache.cachedProcessDone.L.Unlock()

		// The ObjectInfoReader is never dirty, so it should remain open.
		_, err = batchProcess.Info(ctx, "refs/heads/master")
		require.NoError(t, err)
		// On the other hand,  the ObjectReader is dirty and should thus have been killed.
		_, err = io.ReadAll(object)
		require.True(t, errors.Is(err, os.ErrClosed))
	})

	t.Run("closed process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		batchProcess, err := cache.BatchProcess(ctx, repoExecutor)
		require.NoError(t, err)

		batch, ok := batchProcess.(*batch)
		require.True(t, ok, "expected batch")

		// Closed processes naturally cannot be reused anymore and thus shouldn't ever get
		// cached.
		batch.objectReader.close()
		batch.objectInfoReader.close()

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		require.Empty(t, keys(t, &cache.objectReaders))
		require.Empty(t, keys(t, &cache.objectInfoReaders))
	})
}

func TestCache_ObjectReader(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)
	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(time.Hour, 10, time.Hour)
	defer cache.Stop()
	cache.cachedProcessDone = sync.NewCond(&sync.Mutex{})

	t.Run("uncancellable", func(t *testing.T) {
		ctx := context.Background()

		require.PanicsWithValue(t, "empty ctx.Done() in catfile.Batch.New()", func() {
			_, _ = cache.ObjectReader(ctx, repoExecutor)
		})
	})

	t.Run("uncacheable", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		// The context doesn't carry a session ID and is thus uncacheable.
		// The process should never get returned to the cache and must be
		// killed on context cancellation.
		reader, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		objectReaderImpl, ok := reader.(*objectReader)
		require.True(t, ok, "expected object reader")

		cancel()

		// We're cheating a bit here to avoid creating a racy test by reaching into the
		// process and trying to read from its stdout. If the cancel did kill the process as
		// expected, then the stdout should be closed and we'll get an EOF.
		output, err := io.ReadAll(objectReaderImpl.stdout)
		if err != nil {
			require.True(t, errors.Is(err, os.ErrClosed))
		} else {
			require.NoError(t, err)
		}
		require.Empty(t, output)

		require.True(t, reader.isClosed())
		require.Empty(t, keys(t, &cache.objectReaders))
	})

	t.Run("cacheable", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		keys := keys(t, &cache.objectReaders)
		require.Equal(t, []key{{
			sessionID:   "1",
			repoStorage: repo.GetStorageName(),
			repoRelPath: repo.GetRelativePath(),
		}}, keys)

		// Assert that we can still read from the cached process.
		_, err = reader.Object(ctx, "refs/heads/master")
		require.NoError(t, err)
	})

	t.Run("dirty process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// While we request object data, we do not consume it at all. The reader is thus
		// dirty and cannot be reused and shouldn't be returned to the cache.
		object, err := reader.Object(ctx, "refs/heads/master")
		require.NoError(t, err)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		require.Empty(t, keys(t, &cache.objectReaders))

		// The process should be killed now, so reading the object must fail.
		_, err = io.ReadAll(object)
		require.True(t, errors.Is(err, os.ErrClosed))
	})

	t.Run("closed process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, err := cache.ObjectReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Closed processes naturally cannot be reused anymore and thus shouldn't ever get
		// cached.
		reader.close()

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		require.Empty(t, keys(t, &cache.objectReaders))
	})
}

func TestCache_ObjectInfoReader(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)
	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(time.Hour, 10, time.Hour)
	defer cache.Stop()
	cache.cachedProcessDone = sync.NewCond(&sync.Mutex{})

	t.Run("uncancellable", func(t *testing.T) {
		ctx := context.Background()

		require.PanicsWithValue(t, "empty ctx.Done() in catfile.Batch.New()", func() {
			_, _ = cache.ObjectInfoReader(ctx, repoExecutor)
		})
	})

	t.Run("uncacheable", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		// The context doesn't carry a session ID and is thus uncacheable.
		// The process should never get returned to the cache and must be
		// killed on context cancellation.
		reader, err := cache.ObjectInfoReader(ctx, repoExecutor)
		require.NoError(t, err)

		objectInfoReaderImpl, ok := reader.(*objectInfoReader)
		require.True(t, ok, "expected object reader")

		cancel()

		// We're cheating a bit here to avoid creating a racy test by reaching into the
		// process and trying to read from its stdout. If the cancel did kill the process as
		// expected, then the stdout should be closed and we'll get an EOF.
		output, err := io.ReadAll(objectInfoReaderImpl.stdout)
		if err != nil {
			require.True(t, errors.Is(err, os.ErrClosed))
		} else {
			require.NoError(t, err)
		}
		require.Empty(t, output)

		require.True(t, reader.isClosed())
		require.Empty(t, keys(t, &cache.objectInfoReaders))
	})

	t.Run("cacheable", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = correlation.ContextWithCorrelation(ctx, "1")
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, err := cache.ObjectInfoReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		keys := keys(t, &cache.objectInfoReaders)
		require.Equal(t, []key{{
			sessionID:   "1",
			repoStorage: repo.GetStorageName(),
			repoRelPath: repo.GetRelativePath(),
		}}, keys)

		// Assert that we can still read from the cached process.
		_, err = reader.Info(ctx, "refs/heads/master")
		require.NoError(t, err)
	})

	t.Run("closed process does not get cached", func(t *testing.T) {
		defer cache.Evict()

		ctx, cancel := testhelper.Context()
		defer cancel()
		ctx = testhelper.MergeIncomingMetadata(ctx,
			metadata.Pairs(SessionIDField, "1"),
		)

		reader, err := cache.ObjectInfoReader(ctx, repoExecutor)
		require.NoError(t, err)

		// Closed processes naturally cannot be reused anymore and thus shouldn't ever get
		// cached.
		reader.close()

		// Cancel the context such that the process will be considered for return to the
		// cache and wait for the cache to collect it.
		cache.cachedProcessDone.L.Lock()
		cancel()
		defer cache.cachedProcessDone.L.Unlock()
		cache.cachedProcessDone.Wait()

		require.Empty(t, keys(t, &cache.objectInfoReaders))
	})
}

func requireStackValid(t *testing.T, s *stack) {
	s.entriesMutex.Lock()
	defer s.entriesMutex.Unlock()

	for _, ent := range s.entries {
		v := ent.value
		require.False(t, v.isClosed(), "values in cache should not be closed: %v %v", ent, v)
	}
}

func mustCreateCacheable(t *testing.T, cfg config.Cfg, repo repository.GitRepo) (cacheable, func()) {
	t.Helper()

	ctx, cancel := testhelper.Context()
	t.Cleanup(cancel)

	batch, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repo), nil)
	require.NoError(t, err)

	return batch, cancel
}

func mustCreateKey(t *testing.T, sessionID string, repo repository.GitRepo) key {
	t.Helper()

	key, cacheable := newCacheKey(sessionID, repo)
	require.True(t, cacheable)

	return key
}

func keys(t *testing.T, s *stack) []key {
	t.Helper()

	s.entriesMutex.Lock()
	defer s.entriesMutex.Unlock()

	var result []key
	for _, ent := range s.entries {
		result = append(result, ent.key)
	}

	return result
}
