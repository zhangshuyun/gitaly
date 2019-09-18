package catfile

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

var catfileLookupCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_catfile_lookups_total",
		Help: "Git catfile lookups by object type",
	},
	[]string{"type"},
)

const (
	// CacheFeatureFlagKey is the feature flag key for catfile batch caching. This should match
	// what is in gitlab-ce
	CacheFeatureFlagKey = "catfile-cache"

	// SessionIDField is the gRPC metadata field we use to store the gitaly session ID.
	SessionIDField = "gitaly-session-id"
)

func init() {
	prometheus.MustRegister(catfileCacheCounter)
	prometheus.MustRegister(currentCatfileProcesses)
	prometheus.MustRegister(totalCatfileProcesses)
	prometheus.MustRegister(catfileLookupCounter)
}

// Batch abstracts 'git cat-file --batch' and 'git cat-file --batch-check'.
// It lets you retrieve object metadata and raw objects from a Git repo.
//
// A Batch instance can only serve single request at a time. If you want to
// use it across multiple goroutines you need to add your own locking.
type Batch struct {
	*batchCore
	token int
}

type batchCore struct {
	sync.Mutex
	*batchCheck
	*batchProcess
	cancel func()
	closed bool
	token  int
}

func (core *batchCore) lockWithToken(token int) (func(), error) {
	core.Lock()
	if core.token != token {
		core.Unlock()
		return nil, errBatchTokenExpired
	}

	return core.Unlock, nil
}

var errBatchTokenExpired = errors.New("batch token expired")

func (core *batchCore) rotateToken() {
	core.Lock()
	core.token++
	core.batchProcess.setToken(core.token)
	core.Unlock()
}

func (core *batchCore) asBatch() *Batch {
	core.Lock()
	defer core.Unlock()
	return &Batch{batchCore: core, token: core.token}
}

// Info returns an ObjectInfo if spec exists. If spec does not exist the
// error is of type NotFoundError.
func (c *Batch) Info(revspec string) (*ObjectInfo, error) {
	unlock, err := c.batchCore.lockWithToken(c.token)
	if err != nil {
		return nil, err
	}
	defer unlock()

	catfileLookupCounter.WithLabelValues("info").Inc()
	return c.batchCheck.info(revspec)
}

// Tree returns a raw tree object. It is an error if revspec does not
// point to a tree. To prevent this firstuse Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tree(revspec string) (io.Reader, error) {
	catfileLookupCounter.WithLabelValues("tree").Inc()
	return c.batchProcess.reader(c.token, revspec, "tree")
}

// Commit returns a raw commit object. It is an error if revspec does not
// point to a commit. To prevent this first use Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Commit(revspec string) (io.Reader, error) {
	catfileLookupCounter.WithLabelValues("commit").Inc()
	return c.batchProcess.reader(c.token, revspec, "commit")
}

// Blob returns a reader for the requested blob. The entire blob must be
// read before any new objects can be requested from this Batch instance.
//
// It is an error if revspec does not point to a blob. To prevent this
// first use Info to resolve the revspec and check the object type.
func (c *Batch) Blob(revspec string) (io.Reader, error) {
	catfileLookupCounter.WithLabelValues("blob").Inc()
	return c.batchProcess.reader(c.token, revspec, "blob")
}

// Tag returns a raw tag object. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tag(revspec string) (io.Reader, error) {
	catfileLookupCounter.WithLabelValues("tag").Inc()
	return c.batchProcess.reader(c.token, revspec, "tag")
}

// Close closes the writers for batchCheck and batch. This is only used for
// cached Batches
func (core *batchCore) Close() {
	core.Lock()
	defer core.Unlock()

	if core.closed {
		return
	}

	core.closed = true
	if core.cancel != nil {
		// both c.batch and c.batchCheck have goroutines that listen on <ctx.Done()
		// when this is cancelled, it will cause those goroutines to close both writers
		core.cancel()
	}
}

func (core *batchCore) isClosed() bool {
	core.Lock()
	defer core.Unlock()
	return core.closed
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

	sessionID := metadata.GetValue(ctx, SessionIDField)
	if sessionID == "" {
		core, err := newBatchCore(ctx, repoPath, env)
		return core.asBatch(), err
	}

	cacheKey := newCacheKey(sessionID, repo)
	requestDone := ctx.Done()

	if c, ok := cache.Checkout(cacheKey); ok {
		go returnWhenDone(requestDone, cache, cacheKey, c)
		return c.asBatch(), nil
	}

	// if we are using caching, create a fresh context for the new batch
	// and initialize the new batch with a cache key and cancel function
	cacheCtx, cacheCancel := context.WithCancel(context.Background())
	core, err := newBatchCore(cacheCtx, repoPath, env)
	if err != nil {
		return nil, err
	}

	core.cancel = cacheCancel
	go returnWhenDone(requestDone, cache, cacheKey, core)

	return core.asBatch(), nil
}

func returnWhenDone(done <-chan struct{}, bc *batchCache, cacheKey key, core *batchCore) {
	<-done

	core.rotateToken()

	if core.isClosed() {
		return
	}

	if core.hasUnreadData() {
		catfileCacheCounter.WithLabelValues("dirty").Inc()
		core.Close()
		return
	}

	bc.Add(cacheKey, core)
}

var injectSpawnErrors = false

type simulatedBatchSpawnError struct{}

func (simulatedBatchSpawnError) Error() string { return "simulated spawn error" }

func newBatchCore(_ctx context.Context, repoPath string, env []string) (_ *batchCore, err error) {
	ctx, cancel := context.WithCancel(_ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	core := &batchCore{}

	core.batchProcess, err = newBatchProcess(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	core.batchCheck, err = newBatchCheck(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	return core, nil
}
