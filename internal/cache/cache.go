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
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/safe"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
}

// New will create a new Cache with the given Keyer.
func New(cfg config.Cfg, locator storage.Locator, opts ...Option) *Cache {
	var cacheConfig cacheConfig
	for _, opt := range opts {
		opt(&cacheConfig)
	}

	return &Cache{
		storages: cfg.Storages,
		keyer:    newLeaseKeyer(locator),
		af: activeFiles{
			Mutex: &sync.Mutex{},
			m:     map[string]int{},
		},
		cacheConfig: cacheConfig,
	}
}

// ErrReqNotFound indicates the request does not exist within the repo digest
var ErrReqNotFound = errors.New("request digest not found within repo namespace")

// GetStream will fetch the cached stream for a request. It is the
// responsibility of the caller to close the stream when done.
func (c *Cache) GetStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (_ io.ReadCloser, err error) {
	defer func() {
		if err != nil {
			countMiss()
		}
	}()

	countRequest()

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

	return instrumentedReadCloser{respF}, nil
}

type instrumentedReadCloser struct {
	io.ReadCloser
}

func (irc instrumentedReadCloser) Read(p []byte) (n int, err error) {
	n, err = irc.ReadCloser.Read(p)
	countReadBytes(float64(n))
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
			countLoserBytes(float64(n))
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
	countWriteBytes(float64(n))

	if err := sf.Commit(); err != nil {
		errTotal.WithLabelValues("ErrSafefileCommit").Inc()
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
			return countErr(ErrMissingLeaseFile)
		}
		return err
	}

	return nil
}
