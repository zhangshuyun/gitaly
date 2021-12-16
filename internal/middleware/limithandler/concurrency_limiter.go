package limithandler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LimitedFunc represents a function that will be limited
type LimitedFunc func() (resp interface{}, err error)

// ConcurrencyLimiter contains rate limiter state
type ConcurrencyLimiter struct {
	semaphores map[string]*semaphoreReference
	max        int64
	mux        *sync.Mutex
	monitor    ConcurrencyMonitor
}

type semaphoreReference struct {
	tokens chan struct{}
	count  int
}

func (sem *semaphoreReference) acquire(ctx context.Context) error {
	select {
	case sem.tokens <- struct{}{}:
		return nil
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
		c.semaphores[lockKey] = &semaphoreReference{tokens: make(chan struct{}, c.max)}
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
	c.mux.Lock()
	defer c.mux.Unlock()

	return len(c.semaphores)
}

// Limit will limit the concurrency of f
func (c *ConcurrencyLimiter) Limit(ctx context.Context, lockKey string, f LimitedFunc) (interface{}, error) {
	if c.max <= 0 {
		return f()
	}

	start := time.Now()
	c.monitor.Queued(ctx)

	sem := c.getSemaphore(lockKey)
	defer c.putSemaphore(lockKey)

	err := sem.acquire(ctx)
	c.monitor.Dequeued(ctx)
	if err != nil {
		return nil, err
	}
	defer sem.release()

	c.monitor.Enter(ctx, time.Since(start))
	defer c.monitor.Exit(ctx)

	return f()
}

// NewLimiter creates a new rate limiter
func NewLimiter(max int, monitor ConcurrencyMonitor) *ConcurrencyLimiter {
	if monitor == nil {
		monitor = &nullConcurrencyMonitor{}
	}

	return &ConcurrencyLimiter{
		semaphores: make(map[string]*semaphoreReference),
		max:        int64(max),
		mux:        &sync.Mutex{},
		monitor:    monitor,
	}
}
