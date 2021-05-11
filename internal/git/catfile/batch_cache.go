package catfile

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata"
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

func newCacheKey(sessionID string, repo repository.GitRepo) key {
	return key{
		sessionID:   sessionID,
		repoStorage: repo.GetStorageName(),
		repoRelPath: repo.GetRelativePath(),
		repoObjDir:  repo.GetGitObjectDirectory(),
		repoAltDir:  strings.Join(repo.GetGitAlternateObjectDirectories(), ","),
	}
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
}

// BatchCache entries always get added to the back of the list. If the
// list gets too long, we evict entries from the front of the list. When
// an entry gets added it gets an expiry time based on a fixed TTL. A
// monitor goroutine periodically evicts expired entries.
type BatchCache struct {
	entries []*entry
	sync.Mutex

	// maxLen is the maximum number of keys in the cache
	maxLen int

	// ttl is the fixed ttl for cache entries
	ttl time.Duration

	// injectSpawnErrors is used for testing purposes only. If set to true, then spawned batch
	// processes will simulate spawn errors.
	injectSpawnErrors bool

	catfileCacheCounter     *prometheus.CounterVec
	currentCatfileProcesses prometheus.Gauge
	totalCatfileProcesses   prometheus.Counter
	catfileLookupCounter    *prometheus.CounterVec
	catfileCacheMembers     prometheus.Gauge
}

// NewCache creates a new catfile process cache.
func NewCache(cfg config.Cfg) *BatchCache {
	return newCache(defaultBatchfileTTL, cfg.Git.CatfileCacheSize, defaultEvictionInterval)
}

func newCache(ttl time.Duration, maxLen int, refreshInterval time.Duration) *BatchCache {
	if maxLen <= 0 {
		maxLen = defaultMaxLen
	}

	bc := &BatchCache{
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
	}

	go bc.monitor(refreshInterval)
	return bc
}

// Describe describes all metrics exposed by BatchCache.
func (bc *BatchCache) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(bc, descs)
}

// Collect collects all metrics exposed by BatchCache.
func (bc *BatchCache) Collect(metrics chan<- prometheus.Metric) {
	bc.catfileCacheCounter.Collect(metrics)
	bc.currentCatfileProcesses.Collect(metrics)
	bc.totalCatfileProcesses.Collect(metrics)
	bc.catfileLookupCounter.Collect(metrics)
	bc.catfileCacheMembers.Collect(metrics)
}

func (bc *BatchCache) monitor(refreshInterval time.Duration) {
	ticker := time.NewTicker(refreshInterval)

	for range ticker.C {
		bc.enforceTTL(time.Now())
	}
}

// BatchProcess creates a new Batch process for the given repository.
func (bc *BatchCache) BatchProcess(ctx context.Context, repo git.RepositoryExecutor) (Batch, error) {
	if ctx.Done() == nil {
		panic("empty ctx.Done() in catfile.Batch.New()")
	}

	sessionID := metadata.GetValue(ctx, SessionIDField)
	if sessionID == "" {
		c, err := bc.newBatch(ctx, repo)
		if err != nil {
			return nil, err
		}
		return newInstrumentedBatch(c, bc.catfileLookupCounter), err
	}

	cacheKey := newCacheKey(sessionID, repo)
	requestDone := ctx.Done()

	if c, ok := bc.checkout(cacheKey); ok {
		go bc.returnWhenDone(requestDone, cacheKey, c)
		return newInstrumentedBatch(c, bc.catfileLookupCounter), nil
	}

	// if we are using caching, create a fresh context for the new batch
	// and initialize the new batch with a bc key and cancel function
	cacheCtx, cacheCancel := context.WithCancel(context.Background())
	c, err := bc.newBatch(cacheCtx, repo)
	if err != nil {
		cacheCancel()
		return nil, err
	}

	c.cancel = cacheCancel
	go bc.returnWhenDone(requestDone, cacheKey, c)

	return newInstrumentedBatch(c, bc.catfileLookupCounter), nil
}

func (bc *BatchCache) returnWhenDone(done <-chan struct{}, cacheKey key, c *batch) {
	<-done

	if c == nil || c.isClosed() {
		return
	}

	if c.hasUnreadData() {
		bc.catfileCacheCounter.WithLabelValues("dirty").Inc()
		c.Close()
		return
	}

	bc.add(cacheKey, c)
}

// add adds a key, value pair to bc. If there are too many keys in bc
// already add will evict old keys until the length is OK again.
func (bc *BatchCache) add(k key, b *batch) {
	bc.Lock()
	defer bc.Unlock()

	if i, ok := bc.lookup(k); ok {
		bc.catfileCacheCounter.WithLabelValues("duplicate").Inc()
		bc.delete(i, true)
	}

	ent := &entry{key: k, value: b, expiry: time.Now().Add(bc.ttl)}
	bc.entries = append(bc.entries, ent)

	for bc.len() > bc.maxLen {
		bc.evictHead()
	}

	bc.catfileCacheMembers.Set(float64(bc.len()))
}

func (bc *BatchCache) head() *entry { return bc.entries[0] }
func (bc *BatchCache) evictHead()   { bc.delete(0, true) }
func (bc *BatchCache) len() int     { return len(bc.entries) }

// checkout removes a value from bc. After use the caller can re-add the value with bc.Add.
func (bc *BatchCache) checkout(k key) (*batch, bool) {
	bc.Lock()
	defer bc.Unlock()

	i, ok := bc.lookup(k)
	if !ok {
		bc.catfileCacheCounter.WithLabelValues("miss").Inc()
		return nil, false
	}

	bc.catfileCacheCounter.WithLabelValues("hit").Inc()

	ent := bc.entries[i]
	bc.delete(i, false)
	return ent.value, true
}

// enforceTTL evicts all entries older than now, assuming the entry
// expiry times are increasing.
func (bc *BatchCache) enforceTTL(now time.Time) {
	bc.Lock()
	defer bc.Unlock()

	for bc.len() > 0 && now.After(bc.head().expiry) {
		bc.evictHead()
	}
}

// Evict evicts all cached processes from the cache.
func (bc *BatchCache) Evict() {
	bc.Lock()
	defer bc.Unlock()

	for bc.len() > 0 {
		bc.evictHead()
	}
}

func (bc *BatchCache) lookup(k key) (int, bool) {
	for i, ent := range bc.entries {
		if ent.key == k {
			return i, true
		}
	}

	return -1, false
}

func (bc *BatchCache) delete(i int, wantClose bool) {
	ent := bc.entries[i]

	if wantClose {
		ent.value.Close()
	}

	bc.entries = append(bc.entries[:i], bc.entries[i+1:]...)
	bc.catfileCacheMembers.Set(float64(bc.len()))
}
