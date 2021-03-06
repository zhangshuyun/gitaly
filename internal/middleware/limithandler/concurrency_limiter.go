package limithandler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
)

// ErrMaxQueueTime indicates a request has reached the maximum time allowed to wait in the
// concurrency queue.
var ErrMaxQueueTime = errors.New("maximum time in concurrency queue reached")

// ErrMaxQueueSize indicates the concurrency queue has reached its maximum size
var ErrMaxQueueSize = errors.New("maximum queue size reached")

// LimitedFunc represents a function that will be limited
type LimitedFunc func() (resp interface{}, err error)

// QueueTickerCreator is a function that provides a ticker
type QueueTickerCreator func() helper.Ticker

// ConcurrencyLimiter contains rate limiter state
type ConcurrencyLimiter struct {
	semaphores map[string]*semaphoreReference
	// maxPerKey is the maximum number of concurrent operations
	// per lockKey
	maxPerKey int64
	// queued tracks the current number of operations waiting to be picked up
	queued int64
	// queuedLimit is the maximum number of operations allowed to wait in a queued state.
	// subsequent incoming operations will fail with an error.
	queuedLimit         int64
	monitor             ConcurrencyMonitor
	mux                 sync.RWMutex
	maxWaitTickerGetter QueueTickerCreator
}

type semaphoreReference struct {
	tokens    chan struct{}
	count     int
	newTicker QueueTickerCreator
}

func (sem *semaphoreReference) acquire(ctx context.Context) error {
	var ticker helper.Ticker

	if featureflag.ConcurrencyQueueMaxWait.IsEnabled(ctx) &&
		sem.newTicker != nil {
		ticker = sem.newTicker()
	} else {
		ticker = helper.Ticker(helper.NewManualTicker())
	}

	defer ticker.Stop()
	ticker.Reset()

	select {
	case sem.tokens <- struct{}{}:
		return nil
	case <-ticker.C():
		return ErrMaxQueueTime
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sem *semaphoreReference) release() { <-sem.tokens }

// Lazy create a semaphore for the given key
func (c *ConcurrencyLimiter) getSemaphore(lockKey string) *semaphoreReference {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.semaphores[lockKey] == nil {
		c.semaphores[lockKey] = &semaphoreReference{
			tokens:    make(chan struct{}, c.maxPerKey),
			newTicker: c.maxWaitTickerGetter,
		}
	}

	c.semaphores[lockKey].count++
	return c.semaphores[lockKey]
}

func (c *ConcurrencyLimiter) putSemaphore(lockKey string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	ref := c.semaphores[lockKey]
	if ref == nil {
		panic("semaphore should be in the map")
	}

	if ref.count <= 0 {
		panic(fmt.Sprintf("bad semaphore ref count %d", ref.count))
	}

	ref.count--
	if ref.count == 0 {
		delete(c.semaphores, lockKey)
	}
}

func (c *ConcurrencyLimiter) countSemaphores() int {
	c.mux.RLock()
	defer c.mux.RUnlock()

	return len(c.semaphores)
}

func (c *ConcurrencyLimiter) queueInc(ctx context.Context) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if featureflag.ConcurrencyQueueEnforceMax.IsEnabled(ctx) &&
		c.queuedLimit > 0 &&
		c.queued >= c.queuedLimit {
		c.monitor.Dropped(ctx, "max_size")
		return ErrMaxQueueSize
	}

	c.queued++
	return nil
}

func (c *ConcurrencyLimiter) queueDec(decremented *bool) {
	if decremented == nil || *decremented {
		return
	}
	*decremented = true
	c.mux.Lock()
	defer c.mux.Unlock()

	c.queued--
}

// Limit will limit the concurrency of f
func (c *ConcurrencyLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	if c.maxPerKey <= 0 {
		return f()
	}

	var decremented bool

	if err := c.queueInc(ctx); err != nil {
		return nil, err
	}
	defer c.queueDec(&decremented)

	start := time.Now()
	c.monitor.Queued(ctx)

	sem := c.getSemaphore(lockKey)
	defer c.putSemaphore(lockKey)

	err := sem.acquire(ctx)
	c.queueDec(&decremented)

	c.monitor.Dequeued(ctx)
	if err != nil {
		if errors.Is(err, ErrMaxQueueTime) {
			c.monitor.Dropped(ctx, "max_time")
		}
		return nil, err
	}
	defer sem.release()

	c.monitor.Enter(ctx, time.Since(start))
	defer c.monitor.Exit(ctx)

	return f()
}

// NewLimiter creates a new rate limiter
func NewLimiter(perKeyLimit, globalLimit int, maxWaitTickerGetter QueueTickerCreator, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = &nullConcurrencyMonitor{}
	}

	return &ConcurrencyLimiter{
		semaphores:          make(map[string]*semaphoreReference),
		maxPerKey:           int64(perKeyLimit),
		queuedLimit:         int64(globalLimit),
		monitor:             monitor,
		maxWaitTickerGetter: maxWaitTickerGetter,
	}
}
