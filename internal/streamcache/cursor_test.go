package streamcache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursor_notify(t *testing.T) {
	c := newCursor()
	n := c.Subscribe()

	expectNotify(t, n, false)
	require.Equal(t, int64(0), c.Position())

	// Expect no notifications after values <= c.Position()
	c.SetPosition(-1)
	expectNotify(t, n, false)
	require.Equal(t, int64(0), c.Position(), "value remains 0 because -1 <= 0")
	c.SetPosition(0)
	expectNotify(t, n, false)
	require.Equal(t, int64(0), c.Position(), "value remains 0 because 0 <= 0")

	// Expect notification after c.Position() went up
	c.SetPosition(1)
	expectNotify(t, n, true)
	require.Equal(t, int64(1), c.Position())
}

func expectNotify(t *testing.T, n *notifier, expected bool) {
	t.Helper()
	select {
	case <-n.C:
		require.True(t, expected, "unexpected notification")
	default:
		require.False(t, expected, "expected notification, got none")
	}
}

func expectDone(t *testing.T, c *cursor, expected bool) {
	t.Helper()
	select {
	case <-c.Done():
		require.True(t, expected, "expected to be done")
	default:
		require.False(t, expected, "expected to not be done")
	}
}

func TestCursorUnsubscribe(t *testing.T) {
	c := newCursor()
	n1 := c.Subscribe()
	n2 := c.Subscribe()

	expectDone(t, c, false)

	c.SetPosition(1)
	expectNotify(t, n1, true)
	expectNotify(t, n2, true)

	c.Unsubscribe(n2)
	expectDone(t, c, false)

	c.SetPosition(2)
	expectNotify(t, n1, true)
	expectNotify(t, n2, false)

	c.Unsubscribe(n1)
	expectDone(t, c, true)

	c.SetPosition(3)
	expectNotify(t, n1, false)
	expectNotify(t, n2, false)
}

func TestCursor_concurrency(t *testing.T) {
	c := newCursor()

	const N = 1000
	start := make(chan struct{})
	wg := &sync.WaitGroup{}

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			n := c.Subscribe()
			defer c.Unsubscribe(n)

			c.SetPosition(int64(i))
			c.Position()
		}(i)
	}

	close(start)
	wg.Wait()
}

func TestCursorIsDone(t *testing.T) {
	c := newCursor()
	expectDone(t, c, false)
	require.False(t, c.IsDone())

	s := c.Subscribe()
	expectDone(t, c, false)
	require.False(t, c.IsDone())

	c.Unsubscribe(s)
	expectDone(t, c, true)
	require.True(t, c.IsDone())
}
