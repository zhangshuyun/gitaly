package cache

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestStreamDBNaiveKeyer(t *testing.T) {
	expectGetMiss := func(ctx context.Context, cache Cache, req *gitalypb.InfoRefsRequest) {
		_, err := cache.GetStream(ctx, req.Repository, req)
		require.Equal(t, ErrReqNotFound, err)
	}

	expectGetHit := func(ctx context.Context, cache Cache, req *gitalypb.InfoRefsRequest, expectStr string) {
		actualStream, err := cache.GetStream(ctx, req.Repository, req)
		require.NoError(t, err)
		actualBytes, err := io.ReadAll(actualStream)
		require.NoError(t, err)
		require.Equal(t, expectStr, string(actualBytes))
	}

	invalidationEvent := func(ctx context.Context, cache Cache, repo *gitalypb.Repository) {
		lease, err := cache.StartLease(repo)
		require.NoError(t, err)
		// imagine repo being modified here
		require.NoError(t, lease.EndLease(ctx))
	}

	storeAndRetrieve := func(ctx context.Context, cache Cache, req *gitalypb.InfoRefsRequest, expectStr string) {
		require.NoError(t, cache.PutStream(ctx, req.Repository, req, strings.NewReader(expectStr)))
		expectGetHit(ctx, cache, req, expectStr)
	}

	cfg := testcfg.Build(t)

	repo1, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	repo2, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	locator := config.NewLocator(cfg)

	req1 := &gitalypb.InfoRefsRequest{
		Repository: repo1,
	}
	req2 := &gitalypb.InfoRefsRequest{
		Repository: repo2,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = testhelper.SetCtxGrpcMethod(ctx, "InfoRefsUploadPack")

	t.Run("empty cache", func(t *testing.T) {
		cache := New(cfg, locator)

		expectGetMiss(ctx, cache, req1)
		expectGetMiss(ctx, cache, req2)
	})

	t.Run("store and retrieve", func(t *testing.T) {
		cache := New(cfg, locator)
		storeAndRetrieve(ctx, cache, req1, "content-1")
		storeAndRetrieve(ctx, cache, req2, "content-2")
	})

	t.Run("invalidation", func(t *testing.T) {
		cache := New(cfg, locator)

		storeAndRetrieve(ctx, cache, req1, "content-1")
		storeAndRetrieve(ctx, cache, req2, "content-2")

		invalidationEvent(ctx, cache, req1.Repository)

		expectGetMiss(ctx, cache, req1)
		expectGetHit(ctx, cache, req2, "content-2")
	})

	t.Run("overwrite existing entry", func(t *testing.T) {
		cache := New(cfg, locator)

		storeAndRetrieve(ctx, cache, req1, "content-1")

		require.NoError(t, cache.PutStream(ctx, req1.Repository, req1, strings.NewReader("not what you were looking for")))
		expectGetHit(ctx, cache, req1, "not what you were looking for")
	})

	t.Run("feature flags affect caching", func(t *testing.T) {
		cache := New(cfg, locator)

		ctxWithFF := featureflag.IncomingCtxWithFeatureFlag(ctx, featureflag.FeatureFlag{
			Name: "meow",
		}, true)

		storeAndRetrieve(ctx, cache, req1, "default")
		expectGetHit(ctx, cache, req1, "default")
		expectGetMiss(ctxWithFF, cache, req1)

		storeAndRetrieve(ctxWithFF, cache, req1, "flagged")
		expectGetHit(ctxWithFF, cache, req1, "flagged")
		expectGetHit(ctx, cache, req1, "default")
	})

	t.Run("critical section", func(t *testing.T) {
		cache := New(cfg, locator)

		storeAndRetrieve(ctx, cache, req2, "unrelated")

		// Start critical section without closing it.
		repo1Lease, err := cache.StartLease(req1.Repository)
		require.NoError(t, err)

		// Accessing repo cache with open critical section should fail.
		_, err = cache.GetStream(ctx, req1.Repository, req1)
		require.Equal(t, err, ErrPendingExists)
		err = cache.PutStream(ctx, req1.Repository, req1, strings.NewReader("conflict"))
		require.Equal(t, err, ErrPendingExists)

		// Other repo caches should be unaffected.
		expectGetHit(ctx, cache, req2, "unrelated")

		// Opening and closing a new critical section doesn't resolve the issue.
		invalidationEvent(ctx, cache, req1.Repository)
		_, err = cache.GetStream(ctx, req1.Repository, req1)
		require.Equal(t, err, ErrPendingExists)

		// Only completing/removing the pending generation file will allow access.
		require.NoError(t, repo1Lease.EndLease(ctx))
		expectGetMiss(ctx, cache, req1)
	})

	t.Run("nonexisteng repository", func(t *testing.T) {
		cache := New(cfg, locator)

		nonexistentRepo := &gitalypb.Repository{
			StorageName:  repo1.StorageName,
			RelativePath: "does-not-exist",
		}

		// Creating a lease on a repo that doesn't exist yet should succeed.
		_, err := cache.StartLease(nonexistentRepo)
		require.NoError(t, err)
	})
}

func TestLoserCount(t *testing.T) {
	// the test can be contaminate by other tests using the cache, so a
	// dedicated storage location should be used
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("storage-1", "storage-2"))
	cfg := cfgBuilder.Build(t)

	locator := config.NewLocator(cfg)
	cache := New(cfg, locator)

	req := &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			RelativePath: "test",
			StorageName:  "storage-1",
		},
	}
	ctx := testhelper.SetCtxGrpcMethod(context.Background(), "InfoRefsUploadPack")

	leashes := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	errQ := make(chan error)

	wg := &sync.WaitGroup{}
	wg.Add(len(leashes))

	// Run streams concurrently for the same repo and request
	for _, l := range leashes {
		go func(l chan struct{}) { errQ <- cache.PutStream(ctx, req.Repository, req, leashedReader{l, wg}) }(l)
		l <- struct{}{}
	}

	wg.Wait()

	start := int(promtest.ToFloat64(cache.bytesLoserTotals))

	for _, l := range leashes {
		close(l)
		require.NoError(t, <-errQ)
	}

	require.Equal(t, start+len(leashes)-1, int(promtest.ToFloat64(cache.bytesLoserTotals)))
}

type leashedReader struct {
	q  <-chan struct{}
	wg *sync.WaitGroup
}

func (lr leashedReader) Read(p []byte) (n int, err error) {
	_, ok := <-lr.q

	if !ok {
		return 0, io.EOF // on channel close
	}

	lr.wg.Done()
	lr.wg.Wait() // wait for all other readers to sync

	return 1, nil // on receive, return 1 byte read
}
