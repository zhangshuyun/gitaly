package catfile

import (
	"context"
	"fmt"
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

	// SessionIDField is the gRPC metadata field we use to store the gitaly session ID.
	SessionIDField = "gitaly-session-id"
)

// Cache is a cache for git-cat-file(1) processes.
type Cache interface {
	// ObjectReader either creates a new object reader or returns a cached one for the given
	// repository.
	ObjectReader(context.Context, git.RepositoryExecutor) (ObjectReader, error)
	// ObjectInfoReader either creates a new object info reader or returns a cached one for the
	// given repository.
	ObjectInfoReader(context.Context, git.RepositoryExecutor) (ObjectInfoReader, error)
	// Evict evicts all cached processes from the cache.
	Evict()
}

type cacheable interface {
	isClosed() bool
	isDirty() bool
	close()
}

// ProcessCache entries always get added to the back of the list. If the
// list gets too long, we evict entries from the front of the list. When
// an entry gets added it gets an expiry time based on a fixed TTL. A
// monitor goroutine periodically evicts expired entries.
type ProcessCache struct {
	// ttl is the fixed ttl for cache entries
	ttl time.Duration
	// monitorTicker is the ticker used for the monitoring Goroutine.
	monitorTicker *time.Ticker
	monitorDone   chan interface{}

	objectReaders     processes
	objectInfoReaders processes

	catfileCacheCounter     *prometheus.CounterVec
	currentCatfileProcesses prometheus.Gauge
	totalCatfileProcesses   prometheus.Counter
	catfileLookupCounter    *prometheus.CounterVec
	catfileCacheMembers     *prometheus.GaugeVec

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
		ttl: ttl,
		objectReaders: processes{
			maxLen: maxLen,
		},
		objectInfoReaders: processes{
			maxLen: maxLen,
		},
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
		catfileCacheMembers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_catfile_cache_members",
				Help: "Gauge of catfile cache members by process type",
			},
			[]string{"type"},
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
			c.objectReaders.EnforceTTL(time.Now())
			c.objectInfoReaders.EnforceTTL(time.Now())
		case <-c.monitorDone:
			close(c.monitorDone)
			return
		}

		c.reportCacheMembers()
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

// ObjectReader creates a new ObjectReader process for the given repository.
func (c *ProcessCache) ObjectReader(ctx context.Context, repo git.RepositoryExecutor) (ObjectReader, error) {
	cacheable, err := c.getOrCreateProcess(ctx, repo, &c.objectReaders, func(ctx context.Context) (cacheable, error) {
		return newObjectReader(ctx, repo, c.catfileLookupCounter)
	})
	if err != nil {
		return nil, err
	}

	objectReader, ok := cacheable.(ObjectReader)
	if !ok {
		return nil, fmt.Errorf("expected object reader, got %T", objectReader)
	}

	return objectReader, nil
}

// ObjectInfoReader creates a new ObjectInfoReader process for the given repository.
func (c *ProcessCache) ObjectInfoReader(ctx context.Context, repo git.RepositoryExecutor) (ObjectInfoReader, error) {
	cacheable, err := c.getOrCreateProcess(ctx, repo, &c.objectInfoReaders, func(ctx context.Context) (cacheable, error) {
		return newObjectInfoReader(ctx, repo, c.catfileLookupCounter)
	})
	if err != nil {
		return nil, err
	}

	objectInfoReader, ok := cacheable.(ObjectInfoReader)
	if !ok {
		return nil, fmt.Errorf("expected object info reader, got %T", objectInfoReader)
	}

	return objectInfoReader, nil
}

func (c *ProcessCache) getOrCreateProcess(
	ctx context.Context,
	repo repository.GitRepo,
	processes *processes,
	create func(context.Context) (cacheable, error),
) (_ cacheable, returnedErr error) {
	requestDone := ctx.Done()
	if requestDone == nil {
		panic("empty ctx.Done() in catfile.Batch.New()")
	}

	defer c.reportCacheMembers()

	var cancel func()
	cacheKey, isCacheable := newCacheKey(metadata.GetValue(ctx, SessionIDField), repo)
	if isCacheable {
		// We only try to look up cached batch processes in case it is cacheable, which
		// requires a session ID. This is mostly done such that git-cat-file(1) processes
		// from one user cannot interfer with those from another user. The main intent is to
		// disallow trivial denial of service attacks against other users in case it is
		// possible to poison the cache with broken git-cat-file(1) processes.

		if entry, ok := processes.Checkout(cacheKey); ok {
			go c.returnWhenDone(requestDone, processes, cacheKey, entry.value, entry.cancel)
			c.catfileCacheCounter.WithLabelValues("hit").Inc()
			return entry.value, nil
		}

		c.catfileCacheCounter.WithLabelValues("miss").Inc()

		// We have not found any cached process, so we need to create a new one. In this
		// case, we need to detach the process from the current context such that it does
		// not get killed when the current context is done. Note that while we explicitly
		// `close()` processes in case this function fails, we must have a cancellable
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

	batch, err := create(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If we somehow fail after creating a new Batch process, then we want to kill
		// spawned processes right away.
		if returnedErr != nil {
			batch.close()
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
		go c.returnWhenDone(requestDone, processes, cacheKey, batch, cancel)
	}

	return batch, nil
}

func (c *ProcessCache) reportCacheMembers() {
	c.catfileCacheMembers.WithLabelValues("object_reader").Set(float64(c.objectReaders.EntryCount()))
	c.catfileCacheMembers.WithLabelValues("object_info_reader").Set(float64(c.objectInfoReaders.EntryCount()))
}

// Evict evicts all cached processes from the cache.
func (c *ProcessCache) Evict() {
	c.objectReaders.Evict()
	c.objectInfoReaders.Evict()
}

func (c *ProcessCache) returnWhenDone(done <-chan struct{}, p *processes, cacheKey key, value cacheable, cancel func()) {
	<-done

	defer func() {
		c.reportCacheMembers()
		if c.cachedProcessDone != nil {
			c.cachedProcessDone.L.Lock()
			defer c.cachedProcessDone.L.Unlock()
			c.cachedProcessDone.Broadcast()
		}
	}()

	if value == nil || value.isClosed() {
		cancel()
		return
	}

	if value.isDirty() {
		cancel()
		c.catfileCacheCounter.WithLabelValues("dirty").Inc()
		value.close()
		return
	}

	if replaced := p.Add(cacheKey, value, c.ttl, cancel); replaced {
		c.catfileCacheCounter.WithLabelValues("duplicate").Inc()
	}
}

type key struct {
	sessionID   string
	repoStorage string
	repoRelPath string
	repoObjDir  string
	repoAltDir  string
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

type entry struct {
	key
	value  cacheable
	expiry time.Time
	cancel func()
}

type processes struct {
	maxLen int

	entriesMutex sync.Mutex
	entries      []*entry
}

// Add adds a key, value pair to p. If there are too many keys in p
// already add will evict old keys until the length is OK again.
func (p *processes) Add(k key, value cacheable, ttl time.Duration, cancel func()) bool {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	replacedExisting := false
	if i, ok := p.lookup(k); ok {
		p.delete(i, true)
		replacedExisting = true
	}

	ent := &entry{
		key:    k,
		value:  value,
		expiry: time.Now().Add(ttl),
		cancel: cancel,
	}
	p.entries = append(p.entries, ent)

	for len(p.entries) > p.maxLen {
		p.evictHead()
	}

	return replacedExisting
}

// Checkout removes a value from p. After use the caller can re-add the value with p.Add.
func (p *processes) Checkout(k key) (*entry, bool) {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	i, ok := p.lookup(k)
	if !ok {
		return nil, false
	}

	entry := p.entries[i]
	p.delete(i, false)
	return entry, true
}

// EnforceTTL evicts all entries older than now, assuming the entry
// expiry times are increasing.
func (p *processes) EnforceTTL(now time.Time) {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	for len(p.entries) > 0 && now.After(p.head().expiry) {
		p.evictHead()
	}
}

// Evict evicts all cached processes from the cache.
func (p *processes) Evict() {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()

	for len(p.entries) > 0 {
		p.evictHead()
	}
}

// EntryCount returns the number of cached entries. This function will locks the ProcessCache to
// avoid races.
func (p *processes) EntryCount() int {
	p.entriesMutex.Lock()
	defer p.entriesMutex.Unlock()
	return len(p.entries)
}

func (p *processes) head() *entry { return p.entries[0] }
func (p *processes) evictHead()   { p.delete(0, true) }

func (p *processes) lookup(k key) (int, bool) {
	for i, ent := range p.entries {
		if ent.key == k {
			return i, true
		}
	}

	return -1, false
}

func (p *processes) delete(i int, wantClose bool) {
	ent := p.entries[i]

	if wantClose {
		// We first cancel the context such that the process gets a SIGKILL signal. Calling
		// `close()` first may lead to a deadlock given that it waits for the process to
		// exit, which may not happen if it hangs writing data to stdout.
		ent.cancel()
		ent.value.close()
	}

	p.entries = append(p.entries[:i], p.entries[i+1:]...)
}
