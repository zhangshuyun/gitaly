package limithandler

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type counter struct {
	sync.Mutex
	max      int
	current  int
	queued   int
	dequeued int
	enter    int
	exit     int
}

func (c *counter) up() {
	c.Lock()
	defer c.Unlock()

	c.current = c.current + 1
	if c.current > c.max {
		c.max = c.current
	}
}

func (c *counter) down() {
	c.Lock()
	defer c.Unlock()

	c.current = c.current - 1
}

func (c *counter) currentVal() int {
	c.Lock()
	defer c.Unlock()
	return c.current
}

func (c *counter) Queued(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	c.queued++
}

func (c *counter) Dequeued(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	c.dequeued++
}

func (c *counter) Enter(ctx context.Context, acquireTime time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.enter++
}

func (c *counter) Exit(ctx context.Context) {
	c.Lock()
	defer c.Unlock()
	c.exit++
}

func TestLimiter(t *testing.T) {
	tests := []struct {
		name             string
		concurrency      int
		maxConcurrency   int
		iterations       int
		buckets          int
		wantMonitorCalls bool
	}{
		{
			name:             "single",
			concurrency:      1,
			maxConcurrency:   1,
			iterations:       1,
			buckets:          1,
			wantMonitorCalls: true,
		},
		{
			name:             "two-at-a-time",
			concurrency:      100,
			maxConcurrency:   2,
			iterations:       10,
			buckets:          1,
			wantMonitorCalls: true,
		},
		{
			name:             "two-by-two",
			concurrency:      100,
			maxConcurrency:   2,
			iterations:       4,
			buckets:          2,
			wantMonitorCalls: true,
		},
		{
			name:             "no-limit",
			concurrency:      10,
			maxConcurrency:   0,
			iterations:       200,
			buckets:          1,
			wantMonitorCalls: false,
		},
		{
			name:           "wide-spread",
			concurrency:    1000,
			maxConcurrency: 2,
			// We use a long delay here to prevent flakiness in CI. If the delay is
			// too short, the first goroutines to enter the critical section will be
			// gone before we hit the intended maximum concurrency.
			iterations:       40,
			buckets:          50,
			wantMonitorCalls: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedGaugeMax := tt.maxConcurrency * tt.buckets
			if tt.maxConcurrency <= 0 {
				expectedGaugeMax = tt.concurrency
			}

			gauge := &counter{}

			limiter := NewLimiter(tt.maxConcurrency, gauge)
			wg := sync.WaitGroup{}
			wg.Add(tt.concurrency)

			full := sync.NewCond(&sync.Mutex{})

			// primePump waits for the gauge to reach the minimum
			// expected max concurrency so that the limiter is
			// "warmed" up before proceeding with the test
			primePump := func() {
				full.L.Lock()
				defer full.L.Unlock()

				gauge.up()

				if gauge.max >= expectedGaugeMax {
					full.Broadcast()
					return
				}

				full.Wait() // wait until full is broadcast
			}

			// We know of an edge case that can lead to the rate limiter
			// occasionally letting one or two extra goroutines run
			// concurrently.
			for c := 0; c < tt.concurrency; c++ {
				go func(counter int) {
					for i := 0; i < tt.iterations; i++ {
						lockKey := strconv.Itoa((i ^ counter) % tt.buckets)

						_, err := limiter.Limit(context.Background(), lockKey, func() (interface{}, error) {
							primePump()

							current := gauge.currentVal()
							require.True(t, current <= expectedGaugeMax, "Expected the number of concurrent operations (%v) to not exceed the maximum concurrency (%v)", current, expectedGaugeMax)

							require.True(t, limiter.countSemaphores() <= tt.buckets, "Expected the number of semaphores (%v) to be lte number of buckets (%v)", limiter.countSemaphores(), tt.buckets)

							gauge.down()
							return nil, nil
						})
						require.NoError(t, err)
					}

					wg.Done()
				}(c)
			}

			wg.Wait()

			assert.Equal(t, expectedGaugeMax, gauge.max, "Expected maximum concurrency")
			assert.Equal(t, 0, gauge.current)
			assert.Equal(t, 0, limiter.countSemaphores())

			var wantMonitorCallCount int
			if tt.wantMonitorCalls {
				wantMonitorCallCount = tt.concurrency * tt.iterations
			} else {
				wantMonitorCallCount = 0
			}

			assert.Equal(t, wantMonitorCallCount, gauge.enter)
			assert.Equal(t, wantMonitorCallCount, gauge.exit)
			assert.Equal(t, wantMonitorCallCount, gauge.queued)
			assert.Equal(t, wantMonitorCallCount, gauge.dequeued)
		})
	}
}
