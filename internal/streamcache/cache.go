// Package streamcache provides a cache for large blobs (in the order of
// gigabytes). Because storing gigabytes of data is slow, cache entries
// can be streamed on the read end before they have finished on the write
// end. Because storing gigabytes of data is expensive, cache entries
// have a back pressure mechanism: if the readers don't make progress
// reading the data, the writers will block. That way our disk can fill
// up no faster than our readers can read from the cache.
//
// The cache has 3 main parts: Cache (in-memory index), filestore (files
// to store the cached data in because it does not fit in memory), and
// pipe (coordinated IO to one file between one writer and multiple
// readers). A cache entry consists of a key, an maximum age, a
// pipe and the error result of the thing writing to the pipe.
//
// Eviction
//
// There are two eviction goroutines: one for Cache and one for filestore.
// The Cache eviction goroutine evicts entries after a set amount of time,
// and deletes their underlying files too. This is safe because Unix file
// semantics guarantee that readers/writers that are still using those
// files can keep using them. In addition to evicting known cache
// entries, we also have a goroutine at the filestore level which
// performs a directory walk. This will clean up cache files left behind
// by other processes.
package streamcache

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/dontpanic"
)

var (
	cacheIndexSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitaly_streamcache_index_entries",
			Help: "Number of index entries in streamcache",
		},
		[]string{"dir"},
	)
)

// Cache is a cache for large byte streams.
type Cache interface {
	// FindOrCreate finds or creates a cache entry. If the create callback
	// runs, it will be asynchronous and created is set to true. Callers must
	// Close() the returned stream to free underlying resources.
	FindOrCreate(key string, create func(io.Writer) error) (s *Stream, created bool, err error)
	// Stop stops the cleanup goroutines of the cache.
	Stop()
}

var _ = Cache(&TestLoggingCache{})

// TestLogEntry records the result of a cache lookup for testing purposes.
type TestLogEntry struct {
	Key     string
	Created bool
	Err     error
}

// TestLoggingCache wraps a real Cache and logs all its lookups. This is
// not suitable for production because the log will grow indefinitely.
// Use only for testing.
type TestLoggingCache struct {
	Cache
	entries []*TestLogEntry
	m       sync.Mutex
}

// FindOrCreate calls the underlying FindOrCreate method and logs the
// result.
func (tlc *TestLoggingCache) FindOrCreate(key string, create func(io.Writer) error) (s *Stream, created bool, err error) {
	s, created, err = tlc.Cache.FindOrCreate(key, create)

	tlc.m.Lock()
	defer tlc.m.Unlock()
	tlc.entries = append(tlc.entries, &TestLogEntry{Key: key, Created: created, Err: err})
	return s, created, err
}

// Entries returns a reference to the log of entries observed so far.
// This is a reference so the caller should not modify the underlying
// array or its elements.
func (tlc *TestLoggingCache) Entries() []*TestLogEntry {
	tlc.m.Lock()
	defer tlc.m.Unlock()
	return tlc.entries
}

var _ = Cache(NullCache{})

// NullCache is a null implementation of Cache. Every lookup is a miss,
// and it uses no storage.
type NullCache struct{}

// FindOrCreate runs create in a goroutine and lets the caller consume
// the result via the returned stream. The created flag is always true.
func (NullCache) FindOrCreate(key string, create func(io.Writer) error) (s *Stream, created bool, err error) {
	pr, pw := io.Pipe()
	w := newWaiter()
	go func() { w.SetError(runCreate(pw, create)) }()
	return &Stream{reader: pr, waiter: w}, true, nil
}

// Stop is a no-op.
func (NullCache) Stop() {}

type cache struct {
	m          sync.Mutex
	maxAge     time.Duration
	index      map[string]*entry
	createFile func() (namedWriteCloser, error)
	stop       chan struct{}
	stopOnce   sync.Once
	logger     logrus.FieldLogger
	dir        string
}

// New returns a new cache instance.
func New(dir string, maxAge time.Duration, logger logrus.FieldLogger) Cache {
	return newCacheWithSleep(dir, maxAge, time.Sleep, logger)
}

func newCacheWithSleep(dir string, maxAge time.Duration, sleep func(time.Duration), logger logrus.FieldLogger) Cache {
	fs := newFilestore(dir, maxAge, sleep, logger)

	c := &cache{
		maxAge:     maxAge,
		index:      make(map[string]*entry),
		createFile: fs.Create,
		stop:       make(chan struct{}),
		logger:     logger,
		dir:        dir,
	}

	dontpanic.GoForever(1*time.Minute, func() {
		sleepLoop(c.stop, c.maxAge, sleep, c.clean)
	})
	go func() {
		<-c.stop
		fs.Stop()
	}()

	return c
}

func (c *cache) Stop() {
	c.stopOnce.Do(func() { close(c.stop) })
}

func (c *cache) clean() {
	c.m.Lock()
	defer c.m.Unlock()

	var removed []*entry
	cutoff := time.Now().Add(-c.maxAge)
	for k, e := range c.index {
		if e.created.Before(cutoff) {
			c.delete(k)
			removed = append(removed, e)
		}
	}

	// Batch together file removals in a goroutine, without holding the mutex
	go func() {
		for _, e := range removed {
			if err := e.pipe.RemoveFile(); err != nil && !os.IsNotExist(err) {
				c.logger.WithError(err).Error("streamcache: remove file evicted from index")
			}
		}
	}()
}

func (c *cache) delete(key string) {
	delete(c.index, key)
	c.setIndexSize()
}

func (c *cache) setIndexSize() {
	cacheIndexSize.WithLabelValues(c.dir).Set(float64(len(c.index)))
}

func (c *cache) FindOrCreate(key string, create func(io.Writer) error) (s *Stream, created bool, err error) {
	c.m.Lock()
	defer c.m.Unlock()

	if e := c.index[key]; e != nil {
		if s, err := e.Open(); err == nil {
			return s, false, nil
		}

		// In this case err != nil. That is allowed to happen, for instance if
		// the *filestore cleanup goroutine deleted the file already. But let's
		// remove the key from the cache to save the next caller the effort of
		// trying to open this entry.
		c.delete(key)
	}

	s, e, err := c.newEntry(key, create)
	if err != nil {
		return nil, false, err
	}

	c.index[key] = e
	c.setIndexSize()

	return s, true, nil
}

type entry struct {
	key     string
	cache   *cache
	pipe    *pipe
	created time.Time
	waiter  *waiter
}

// Stream abstracts a stream of bytes (via Read()) plus an error (via
// Wait()). Callers must always call Close() to prevent resource leaks.
type Stream struct {
	waiter *waiter
	reader io.ReadCloser
}

// Wait returns the error value of the Stream. If ctx is canceled,
// Wait unblocks and returns early.
func (s *Stream) Wait(ctx context.Context) error { return s.waiter.Wait(ctx) }

// Read reads from the underlying stream of the stream.
func (s *Stream) Read(p []byte) (int, error) { return s.reader.Read(p) }

// Close releases the underlying resources of the stream.
func (s *Stream) Close() error { return s.reader.Close() }

func (c *cache) newEntry(key string, create func(io.Writer) error) (_ *Stream, _ *entry, err error) {
	e := &entry{
		key:     key,
		cache:   c,
		created: time.Now(),
		waiter:  newWaiter(),
	}

	// Every entry gets a unique underlying file. We do not want to reuse
	// existing cache files because we do not know whether they are the
	// result of a succesfull call to create.
	//
	// This may sound like we should be using an anonymous tempfile, but that
	// would be at odds with the requirement to be able to open and close
	// multiple instances of the file independently: one for the writer, and
	// one for each reader.
	//
	// So the name of the file is irrelevant, but the file must have _a_
	// name.
	f, err := c.createFile()
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	var pr io.ReadCloser
	pr, e.pipe, err = newPipe(f)
	if err != nil {
		return nil, nil, err
	}

	go func() {
		err := runCreate(e.pipe, create)
		e.waiter.SetError(err)
		if err != nil {
			c.logger.WithError(err).Error("create cache entry")
			c.m.Lock()
			defer c.m.Unlock()
			c.delete(key)
		}
	}()

	return e.wrapReadCloser(pr), e, nil
}

func (e *entry) wrapReadCloser(r io.ReadCloser) *Stream {
	return &Stream{reader: r, waiter: e.waiter}
}

func runCreate(w io.WriteCloser, create func(io.Writer) error) (err error) {
	// Catch panics because this function runs in a goroutine. That means that
	// unlike RPC handlers, which are guarded by a panic catching middleware,
	// an uncaught panic can crash the whole process.
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()

	defer w.Close()

	if err := create(w); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

func (e *entry) Open() (*Stream, error) {
	r, err := e.pipe.OpenReader()
	return e.wrapReadCloser(r), err
}

type waiter struct {
	done chan struct{}
	err  error
	once sync.Once
}

func newWaiter() *waiter { return &waiter{done: make(chan struct{})} }

func (w *waiter) SetError(err error) {
	w.once.Do(func() {
		w.err = err
		close(w.done)
	})
}

func (w *waiter) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return w.err
	}
}

func sleepLoop(done chan struct{}, period time.Duration, sleep func(time.Duration), callback func()) {
	const maxPeriod = time.Minute
	if period <= 0 || period >= maxPeriod {
		period = maxPeriod
	}

	for {
		sleep(period)

		select {
		case <-done:
			return
		default:
		}

		callback()
	}
}
