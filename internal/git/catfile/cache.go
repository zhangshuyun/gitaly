package catfile

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// defaultBatchfileTTL is the default ttl for batch files to live in the cache
	defaultBatchfileTTL = 10 * time.Second

	defaultEvictionInterval = 1 * time.Second

	// The default maximum number of cache entries
	defaultMaxLen = 100
)

// Cache is a cache for git-cat-file(1) processes.
type Cache interface {
	// BatchProcess either creates a new git-cat-file(1) process or returns a cached one for
	// the given repository.
	BatchProcess(context.Context, git.RepositoryExecutor) (Batch, error)
	// Evict evicts all cached processes from the cache.
	Evict()
}

func newCacheKey(sessionID string, repo repository.GitRepo) (key, bool) {
	if sessionID == "" {
		return key{}, false
	}

	return key{
		sessionID:   sessionID,
		repoStorage: repo.GetStorageName(),
		repoRelPath: repo.GetRelativePath(),
		repoObjDir:  repo.GetGitObjectDirectory(),
		repoAltDir:  strings.Join(repo.GetGitAlternateObjectDirectories(), ","),
	}, true
}

type key struct {
	sessionID   string
	repoStorage string
	repoRelPath string
	repoObjDir  string
	repoAltDir  string
}

type entry struct {
	key
	value  *batch
	expiry time.Time
	cancel func()
}

// ProcessCache entries always get added to the back of the list. If the
// list gets too long, we evict entries from the front of the list. When
// an entry gets added it gets an expiry time based on a fixed TTL. A
// monitor goroutine periodically evicts expired entries.
type ProcessCache struct {
	// maxLen is the maximum number of keys in the cache
	maxLen int
	// ttl is the fixed ttl for cache entries
	ttl time.Duration
	// monitorTicker is the ticker used for the monitoring Goroutine.
	monitorTicker *time.Ticker
	monitorDone   chan interface{}

	catfileCacheCounter     *prometheus.CounterVec
	currentCatfileProcesses prometheus.Gauge
	totalCatfileProcesses   prometheus.Counter
	catfileLookupCounter    *prometheus.CounterVec
	catfileCacheMembers     prometheus.Gauge

	entriesMutex sync.Mutex
	entries      []*entry

	// cachedProcessDone is a condition that gets signalled whenever a process is being
	// considered to be returned to the cache. This field is optional and must only be used in
	// tests.
	cachedProcessDone *sync.Cond
}

// NewCache creates a new catfile process cache.
func NewCache(cfg config.Cfg) *ProcessCache {
	return newCache(defaultBatchfileTTL, cfg.Git.CatfileCacheSize, defaultEvictionInterval)
}

func newCache(ttl time.Duration, maxLen int, refreshInterval time.Duration) *ProcessCache {
	if maxLen <= 0 {
		maxLen = defaultMaxLen
	}

	processCache := &ProcessCache{
		maxLen: maxLen,
		ttl:    ttl,
		catfileCacheCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_catfile_cache_total",
				Help: "Counter of catfile cache hit/miss",
			},
			[]string{"type"},
		),
		currentCatfileProcesses: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "gitaly_catfile_processes",
				Help: "Gauge of active catfile processes",
			},
		),
		totalCatfileProcesses: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_catfile_processes_total",
				Help: "Counter of catfile processes",
			},
		),
		catfileLookupCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_catfile_lookups_total",
				Help: "Git catfile lookups by object type",
			},
			[]string{"type"},
		),
		catfileCacheMembers: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "gitaly_catfile_cache_members",
				Help: "Gauge of catfile cache members",
			},
		),
		monitorTicker: time.NewTicker(refreshInterval),
		monitorDone:   make(chan interface{}),
	}

	go processCache.monitor()
	return processCache
}

// Describe describes all metrics exposed by ProcessCache.
func (c *ProcessCache) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect collects all metrics exposed by ProcessCache.
func (c *ProcessCache) Collect(metrics chan<- prometheus.Metric) {
	c.catfileCacheCounter.Collect(metrics)
	c.currentCatfileProcesses.Collect(metrics)
	c.totalCatfileProcesses.Collect(metrics)
	c.catfileLookupCounter.Collect(metrics)
	c.catfileCacheMembers.Collect(metrics)
}

func (c *ProcessCache) monitor() {
	for {
		select {
		case <-c.monitorTicker.C:
			c.enforceTTL(time.Now())
		case <-c.monitorDone:
			close(c.monitorDone)
			return
		}
	}
}

// Stop stops the monitoring Goroutine and evicts all cached processes. This must only be called
// once.
func (c *ProcessCache) Stop() {
	c.monitorTicker.Stop()
	c.monitorDone <- struct{}{}
	<-c.monitorDone
	c.Evict()
}

// BatchProcess creates a new Batch process for the given repository.
func (c *ProcessCache) BatchProcess(ctx context.Context, repo git.RepositoryExecutor) (_ Batch, returnedErr error) {
	requestDone := ctx.Done()
	if requestDone == nil {
		panic("empty ctx.Done() in catfile.Batch.New()")
	}

	var cancel func()
	cacheKey, isCacheable := newCacheKey(metadata.GetValue(ctx, SessionIDField), repo)
	if isCacheable {
		// We only try to look up cached batch processes in case it is cacheable, which
		// requires a session ID. This is mostly done such that git-cat-file(1) processes
		// from one user cannot interfer with those from another user. The main intent is to
		// disallow trivial denial of service attacks against other users in case it is
		// possible to poison the cache with broken git-cat-file(1) processes.

		if entry, ok := c.checkout(cacheKey); ok {
			go c.returnWhenDone(requestDone, cacheKey, entry.value, entry.cancel)
			return entry.value, nil
		}

		// We have not found any cached process, so we need to create a new one. In this
		// case, we need to detach the process from the current context such that it does
		// not get killed when the current context is done. Note that while we explicitly
		// `Close()` processes in case this function fails, we must have a cancellable
		// context or otherwise our `command` package will panic.
		ctx, cancel = context.WithCancel(context.Background())
		defer func() {
			if returnedErr != nil {
				cancel()
			}
		}()

		// We have to decorrelate the process from the current context given that it
		// may potentially be reused across different RPC calls.
		ctx = correlation.ContextWithCorrelation(ctx, "")
		ctx = opentracing.ContextWithSpan(ctx, nil)
	}

	batch, err := newBatch(ctx, repo, c.catfileLookupCounter)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If we somehow fail after creating a new Batch process, then we want to kill
		// spawned processes right away.
		if returnedErr != nil {
			batch.Close()
		}
	}()

	c.totalCatfileProcesses.Inc()
	c.currentCatfileProcesses.Inc()
	go func() {
		<-ctx.Done()
		c.currentCatfileProcesses.Dec()
	}()

	if isCacheable {
		// If the process is cacheable, then we want to put the process into the cache when
		// the current outer context is done.
		go c.returnWhenDone(requestDone, cacheKey, batch, cancel)
	}

	return batch, nil
}

func (c *ProcessCache) returnWhenDone(done <-chan struct{}, cacheKey key, batch *batch, cancel func()) {
	<-done

	if c.cachedProcessDone != nil {
		defer func() {
			c.cachedProcessDone.Broadcast()
		}()
	}

	if batch == nil || batch.isClosed() {
		cancel()
		return
	}

	if batch.hasUnreadData() {
		cancel()
		c.catfileCacheCounter.WithLabelValues("dirty").Inc()
		batch.Close()
		return
	}

	c.add(cacheKey, batch, cancel)
}

// add adds a key, value pair to c. If there are too many keys in c
// already add will evict old keys until the length is OK again.
func (c *ProcessCache) add(k key, b *batch, cancel func()) {
	c.entriesMutex.Lock()
	defer c.entriesMutex.Unlock()

	if i, ok := c.lookup(k); ok {
		c.catfileCacheCounter.WithLabelValues("duplicate").Inc()
		c.delete(i, true)
	}

	ent := &entry{
		key:    k,
		value:  b,
		expiry: time.Now().Add(c.ttl),
		cancel: cancel,
	}
	c.entries = append(c.entries, ent)

	for c.len() > c.maxLen {
		c.evictHead()
	}

	c.catfileCacheMembers.Set(float64(c.len()))
}

func (c *ProcessCache) head() *entry { return c.entries[0] }
func (c *ProcessCache) evictHead()   { c.delete(0, true) }
func (c *ProcessCache) len() int     { return len(c.entries) }

// checkout removes a value from c. After use the caller can re-add the value with c.Add.
func (c *ProcessCache) checkout(k key) (*entry, bool) {
	c.entriesMutex.Lock()
	defer c.entriesMutex.Unlock()

	i, ok := c.lookup(k)
	if !ok {
		c.catfileCacheCounter.WithLabelValues("miss").Inc()
		return nil, false
	}

	c.catfileCacheCounter.WithLabelValues("hit").Inc()

	entry := c.entries[i]
	c.delete(i, false)
	return entry, true
}

// enforceTTL evicts all entries older than now, assuming the entry
// expiry times are increasing.
func (c *ProcessCache) enforceTTL(now time.Time) {
	c.entriesMutex.Lock()
	defer c.entriesMutex.Unlock()

	for c.len() > 0 && now.After(c.head().expiry) {
		c.evictHead()
	}
}

// Evict evicts all cached processes from the cache.
func (c *ProcessCache) Evict() {
	c.entriesMutex.Lock()
	defer c.entriesMutex.Unlock()

	for c.len() > 0 {
		c.evictHead()
	}
}

func (c *ProcessCache) lookup(k key) (int, bool) {
	for i, ent := range c.entries {
		if ent.key == k {
			return i, true
		}
	}

	return -1, false
}

func (c *ProcessCache) delete(i int, wantClose bool) {
	ent := c.entries[i]

	if wantClose {
		ent.value.Close()
		ent.cancel()
	}

	c.entries = append(c.entries[:i], c.entries[i+1:]...)
	c.catfileCacheMembers.Set(float64(c.len()))
}
