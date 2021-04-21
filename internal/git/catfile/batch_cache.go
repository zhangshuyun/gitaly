package catfile

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

const (
	// DefaultBatchfileTTL is the default ttl for batch files to live in the cache
	DefaultBatchfileTTL = 10 * time.Second

	defaultEvictionInterval = 1 * time.Second

	// The default maximum number of cache entries
	defaultMaxLen = 100
)

var catfileCacheMembers = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "gitaly_catfile_cache_members",
		Help: "Gauge of catfile cache members",
	},
)

var cache *batchCache

func init() {
	config.RegisterHook(func(cfg *config.Cfg) error {
		cache = newCache(DefaultBatchfileTTL, cfg.Git.CatfileCacheSize)
		return nil
	})
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

// batchCache entries always get added to the back of the list. If the
// list gets too long, we evict entries from the front of the list. When
// an entry gets added it gets an expiry time based on a fixed TTL. A
// monitor goroutine periodically evicts expired entries.
type batchCache struct {
	entries []*entry
	sync.Mutex

	// maxLen is the maximum number of keys in the cache
	maxLen int

	// ttl is the fixed ttl for cache entries
	ttl time.Duration
}

func newCache(ttl time.Duration, maxLen int) *batchCache {
	return newCacheWithRefresh(ttl, maxLen, defaultEvictionInterval)
}

func newCacheWithRefresh(ttl time.Duration, maxLen int, refreshInterval time.Duration) *batchCache {
	if maxLen <= 0 {
		maxLen = defaultMaxLen
	}

	bc := &batchCache{
		maxLen: maxLen,
		ttl:    ttl,
	}

	go bc.monitor(refreshInterval)
	return bc
}

func (bc *batchCache) monitor(refreshInterval time.Duration) {
	ticker := time.NewTicker(refreshInterval)

	for range ticker.C {
		bc.EnforceTTL(time.Now())
	}
}

// Add adds a key, value pair to bc. If there are too many keys in bc
// already Add will evict old keys until the length is OK again.
func (bc *batchCache) Add(k key, b *batch) {
	bc.Lock()
	defer bc.Unlock()

	if i, ok := bc.lookup(k); ok {
		catfileCacheCounter.WithLabelValues("duplicate").Inc()
		bc.delete(i, true)
	}

	ent := &entry{key: k, value: b, expiry: time.Now().Add(bc.ttl)}
	bc.entries = append(bc.entries, ent)

	for bc.len() > bc.maxLen {
		bc.evictHead()
	}

	catfileCacheMembers.Set(float64(bc.len()))
}

func (bc *batchCache) head() *entry { return bc.entries[0] }
func (bc *batchCache) evictHead()   { bc.delete(0, true) }
func (bc *batchCache) len() int     { return len(bc.entries) }

// Checkout removes a value from bc. After use the caller can re-add the value with bc.Add.
func (bc *batchCache) Checkout(k key) (*batch, bool) {
	bc.Lock()
	defer bc.Unlock()

	i, ok := bc.lookup(k)
	if !ok {
		catfileCacheCounter.WithLabelValues("miss").Inc()
		return nil, false
	}

	catfileCacheCounter.WithLabelValues("hit").Inc()

	ent := bc.entries[i]
	bc.delete(i, false)
	return ent.value, true
}

// EnforceTTL evicts all entries older than now, assuming the entry
// expiry times are increasing.
func (bc *batchCache) EnforceTTL(now time.Time) {
	bc.Lock()
	defer bc.Unlock()

	for bc.len() > 0 && now.After(bc.head().expiry) {
		bc.evictHead()
	}
}

func (bc *batchCache) EvictAll() {
	bc.Lock()
	defer bc.Unlock()

	for bc.len() > 0 {
		bc.evictHead()
	}
}

// ExpireAll is used to expire all of the batches in the cache
func ExpireAll() {
	cache.EvictAll()
}

func (bc *batchCache) lookup(k key) (int, bool) {
	for i, ent := range bc.entries {
		if ent.key == k {
			return i, true
		}
	}

	return -1, false
}

func (bc *batchCache) delete(i int, wantClose bool) {
	ent := bc.entries[i]

	if wantClose {
		ent.value.Close()
	}

	bc.entries = append(bc.entries[:i], bc.entries[i+1:]...)
	catfileCacheMembers.Set(float64(bc.len()))
}
