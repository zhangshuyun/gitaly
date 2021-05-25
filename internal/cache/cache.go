package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// maps a cache path to the number of active writers
type activeFiles struct {
	*sync.Mutex
	m map[string]int
}

// trackFile returns a function that indicates if the current
// writing of a file is the last known one, which
// would indicate the current write is the "winner".
func (af activeFiles) trackFile(path string) func() bool {
	af.Lock()
	defer af.Unlock()

	af.m[path]++

	return func() bool {
		af.Lock()
		defer af.Unlock()

		af.m[path]--

		winner := af.m[path] == 0
		if winner {
			delete(af.m, path) // reclaim memory
		}

		return winner
	}
}

type cacheConfig struct {
	disableMoveAndClear bool // only used to disable move and clear in tests
	disableWalker       bool // only used to disable object walker in tests
}

// Option is an option for the cache.
type Option func(*cacheConfig)

// withDisabledMoveAndClear disables the initial move and cleanup of preexisting cache directories.
// This option is only for test purposes.
func withDisabledMoveAndClear() Option {
	return func(cfg *cacheConfig) {
		cfg.disableMoveAndClear = true
	}
}

// withDisabledWalker disables the cache walker which cleans up the cache asynchronously. This
// option is only for test purposes.
func withDisabledWalker() Option {
	return func(cfg *cacheConfig) {
		cfg.disableWalker = true
	}
}

// Cache stores and retrieves byte streams for repository related RPCs
type Cache struct {
	storages    []config.Storage
	keyer       leaseKeyer
	af          activeFiles
	cacheConfig cacheConfig

	requestTotals              prometheus.Counter
	missTotals                 prometheus.Counter
	bytesStoredtotals          prometheus.Counter
	bytesFetchedtotals         prometheus.Counter
	bytesLoserTotals           prometheus.Counter
	errTotal                   *prometheus.CounterVec
	walkerCheckTotal           prometheus.Counter
	walkerRemovalTotal         prometheus.Counter
	walkerErrorTotal           prometheus.Counter
	walkerEmptyDirTotal        prometheus.Counter
	walkerEmptyDirRemovalTotal prometheus.Counter
}

// New will create a new Cache with the given Keyer.
func New(cfg config.Cfg, locator storage.Locator, opts ...Option) *Cache {
	var cacheConfig cacheConfig
	for _, opt := range opts {
		opt(&cacheConfig)
	}

	cache := &Cache{
		storages: cfg.Storages,
		af: activeFiles{
			Mutex: &sync.Mutex{},
			m:     map[string]int{},
		},
		cacheConfig: cacheConfig,

		requestTotals: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_requests_total",
				Help: "Total number of disk cache requests",
			},
		),
		missTotals: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_miss_total",
				Help: "Total number of disk cache misses",
			},
		),
		bytesStoredtotals: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_bytes_stored_total",
				Help: "Total number of disk cache bytes stored",
			},
		),
		bytesFetchedtotals: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_bytes_fetched_total",
				Help: "Total number of disk cache bytes fetched",
			},
		),
		bytesLoserTotals: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_bytes_loser_total",
				Help: "Total number of disk cache bytes from losing writes",
			},
		),
		errTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_errors_total",
				Help: "Total number of errors encountered by disk cache",
			},
			[]string{"error"},
		),
		walkerCheckTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_walker_check_total",
				Help: "Total number of events during diskcache filesystem walks",
			},
		),
		walkerRemovalTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_walker_removal_total",
				Help: "Total number of events during diskcache filesystem walks",
			},
		),
		walkerErrorTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_walker_error_total",
				Help: "Total number of errors during diskcache filesystem walks",
			},
		),
		walkerEmptyDirTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_walker_empty_dir_total",
				Help: "Total number of empty directories encountered",
			},
		),
		walkerEmptyDirRemovalTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "gitaly_diskcache_walker_empty_dir_removal_total",
				Help: "Total number of empty directories removed",
			},
		),
	}
	cache.keyer = newLeaseKeyer(locator, cache.countErr)

	return cache
}

// Describe is used to describe Prometheus metrics.
func (c *Cache) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect is used to collect Prometheus metrics.
func (c *Cache) Collect(metrics chan<- prometheus.Metric) {
	c.requestTotals.Collect(metrics)
	c.missTotals.Collect(metrics)
	c.bytesStoredtotals.Collect(metrics)
	c.bytesFetchedtotals.Collect(metrics)
	c.bytesLoserTotals.Collect(metrics)
	c.errTotal.Collect(metrics)
	c.walkerRemovalTotal.Collect(metrics)
	c.walkerErrorTotal.Collect(metrics)
	c.walkerEmptyDirTotal.Collect(metrics)
	c.walkerEmptyDirRemovalTotal.Collect(metrics)
}

func (c *Cache) countErr(err error) error {
	switch err {
	case ErrMissingLeaseFile:
		c.errTotal.WithLabelValues("ErrMissingLeaseFile").Inc()
	case ErrPendingExists:
		c.errTotal.WithLabelValues("ErrPendingExists").Inc()
	}
	return err
}

// ErrReqNotFound indicates the request does not exist within the repo digest
var ErrReqNotFound = errors.New("request digest not found within repo namespace")

// GetStream will fetch the cached stream for a request. It is the
// responsibility of the caller to close the stream when done.
func (c *Cache) GetStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (_ io.ReadCloser, err error) {
	defer func() {
		if err != nil {
			c.missTotals.Inc()
		}
	}()

	c.requestTotals.Inc()

	respPath, err := c.KeyPath(ctx, repo, req)
	switch {
	case os.IsNotExist(err):
		return nil, ErrReqNotFound
	case err == nil:
		break
	default:
		return nil, err
	}

	ctxlogrus.Extract(ctx).
		WithField("stream_path", respPath).
		Info("getting stream")

	respF, err := os.Open(respPath)
	switch {
	case os.IsNotExist(err):
		return nil, ErrReqNotFound
	case err == nil:
		break
	default:
		return nil, err
	}

	return instrumentedReadCloser{
		ReadCloser: respF,
		counter:    c.bytesFetchedtotals,
	}, nil
}

type instrumentedReadCloser struct {
	io.ReadCloser
	counter prometheus.Counter
}

func (irc instrumentedReadCloser) Read(p []byte) (n int, err error) {
	n, err = irc.ReadCloser.Read(p)
	irc.counter.Add(float64(n))
	return
}

// PutStream will store a stream in a repo-namespace keyed by the digest of the
// request protobuf message.
func (c *Cache) PutStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message, src io.Reader) error {
	reqPath, err := c.KeyPath(ctx, repo, req)
	if err != nil {
		return err
	}

	ctxlogrus.Extract(ctx).
		WithField("stream_path", reqPath).
		Info("putting stream")

	var n int64
	isWinner := c.af.trackFile(reqPath)
	defer func() {
		if !isWinner() {
			c.bytesLoserTotals.Add(float64(n))
		}
	}()

	if err := os.MkdirAll(filepath.Dir(reqPath), 0755); err != nil {
		return err
	}

	sf, err := safe.CreateFileWriter(reqPath)
	if err != nil {
		return err
	}
	defer sf.Close()

	n, err = io.Copy(sf, src)
	if err != nil {
		return err
	}
	c.bytesStoredtotals.Add(float64(n))

	if err := sf.Commit(); err != nil {
		c.errTotal.WithLabelValues("ErrSafefileCommit").Inc()
		return err
	}

	return nil
}

// KeyPath returns the cache path for the given request.
func (c *Cache) KeyPath(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (string, error) {
	return c.keyer.keyPath(ctx, repo, req)
}

// StartLease will mark the repository as being in an indeterministic state. This is typically used
// when modifying the repo, since the cache is not stable until after the modification is complete.
// A lease object will be returned that allows the caller to signal the end of the lease.
func (c *Cache) StartLease(repo *gitalypb.Repository) (LeaseEnder, error) {
	pendingPath, err := c.keyer.newPendingLease(repo)
	if err != nil {
		return lease{}, err
	}

	return lease{
		pendingPath: pendingPath,
		repo:        repo,
		keyer:       c.keyer,
		countErr:    c.countErr,
	}, nil
}

// LeaseEnder allows the caller to indicate when a lease is no longer needed
type LeaseEnder interface {
	EndLease(context.Context) error
}

type lease struct {
	pendingPath string
	repo        *gitalypb.Repository
	keyer       leaseKeyer
	countErr    func(error) error
}

// EndLease will end the lease by removing the pending lease file and updating
// the key file with the current lease ID.
func (l lease) EndLease(ctx context.Context) error {
	_, err := l.keyer.updateLatest(ctx, l.repo)
	if err != nil {
		return err
	}

	if err := os.Remove(l.pendingPath); err != nil {
		if os.IsNotExist(err) {
			return l.countErr(ErrMissingLeaseFile)
		}
		return err
	}

	return nil
}
