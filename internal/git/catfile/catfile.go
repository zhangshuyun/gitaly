package catfile

import (
	"context"
	"io"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
)

var catfileCacheCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_catfile_cache_total",
		Help: "Counter of catfile cache hit/miss",
	},
	[]string{"type"},
)

var currentCatfileProcesses = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "gitaly_catfile_processes",
		Help: "Gauge of active catfile processes",
	},
)

var totalCatfileProcesses = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "gitaly_catfile_processes_total",
		Help: "Counter of catfile processes",
	},
)

func init() {
	prometheus.MustRegister(catfileCacheCounter)
	prometheus.MustRegister(currentCatfileProcesses)
	prometheus.MustRegister(totalCatfileProcesses)
}

// Batch abstracts 'git cat-file --batch' and 'git cat-file --batch-check'.
// It lets you retrieve object metadata and raw objects from a Git repo.
//
// A Batch instance can only serve single request at a time. If you want to
// use it across multiple goroutines you need to add your own locking.
type Batch struct {
	sync.Mutex
	*batchCheck
	*batchProcess
	cancel func()
	closed bool
}

// Info returns an ObjectInfo if spec exists. If spec does not exist the
// error is of type NotFoundError.
func (c *Batch) Info(revspec string) (*ObjectInfo, error) {
	return c.batchCheck.info(revspec)
}

// Tree returns a raw tree object. It is an error if revspec does not
// point to a tree. To prevent this firstuse Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tree(revspec string) (io.Reader, error) {
	return c.batchProcess.reader(revspec, "tree")
}

// Commit returns a raw commit object. It is an error if revspec does not
// point to a commit. To prevent this first use Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Commit(revspec string) (io.Reader, error) {
	return c.batchProcess.reader(revspec, "commit")
}

// Blob returns a reader for the requested blob. The entire blob must be
// read before any new objects can be requested from this Batch instance.
//
// It is an error if revspec does not point to a blob. To prevent this
// first use Info to resolve the revspec and check the object type.
func (c *Batch) Blob(revspec string) (io.Reader, error) {
	return c.batchProcess.reader(revspec, "blob")
}

// Tag returns a raw tag object. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tag(revspec string) (io.Reader, error) {
	return c.batchProcess.reader(revspec, "tag")
}

// Close closes the writers for batchCheck and batch. This is only used for
// cached Batches
func (c *Batch) Close() {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	if c.cancel != nil {
		// both c.batch and c.batchCheck have goroutines that listen on <ctx.Done()
		// when this is cancelled, it will cause those goroutines to close both writers
		c.cancel()
	}
}

func (c *Batch) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

// New returns a new Batch instance. It is important that ctx gets canceled
// somewhere, because if it doesn't the cat-file processes spawned by
// New() never terminate.
func New(ctx context.Context, repo *gitalypb.Repository) (*Batch, error) {
	if ctx.Done() == nil {
		panic("empty ctx.Done() in catfile.Batch.New()")
	}

	repoPath, env, err := alternates.PathAndEnv(repo)
	if err != nil {
		return nil, err
	}

	sessionID := metadata.GetValue(ctx, "gitaly-session-id")

	if featureflag.IsDisabled(ctx, featureflag.CatfileCacheKey) || sessionID == "" {
		return newBatch(ctx, repoPath, env)
	}

	cacheKey := newCacheKey(sessionID, repo)
	requestDone := ctx.Done()

	if c, ok := cache.Checkout(cacheKey); ok {
		go returnWhenDone(requestDone, cache, cacheKey, c)
		return c, nil
	}

	// if we are using caching, create a fresh context for the new batch
	// and initialize the new batch with a cache key and cancel function
	cacheCtx, cacheCancel := context.WithCancel(context.Background())
	c, err := newBatch(cacheCtx, repoPath, env)
	if err != nil {
		return nil, err
	}

	c.cancel = cacheCancel
	go returnWhenDone(requestDone, cache, cacheKey, c)

	return c, nil
}

func returnWhenDone(done <-chan struct{}, bc *batchCache, cacheKey key, c *Batch) {
	<-done

	if c == nil || c.isClosed() {
		return
	}

	if c.hasUnreadData() {
		catfileCacheCounter.WithLabelValues("dirty").Inc()
		c.Close()
		return
	}

	bc.Add(cacheKey, c)
}

func newBatch(ctx context.Context, repoPath string, env []string) (*Batch, error) {
	batch, err := newBatchProcess(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	batchCheck, err := newBatchCheck(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	return &Batch{batchProcess: batch, batchCheck: batchCheck}, nil
}
