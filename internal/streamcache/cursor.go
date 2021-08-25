package streamcache

import (
	"sync"
)

// cursor is a datatype that combines concurrent updates of an int64 with
// change notifications. The number is only allowed to go up; it is meant
// to represent the read or write offset in a file that is being accessed
// linearly.
type cursor struct {
	pos         int64
	subscribers []*notifier
	m           sync.RWMutex
	doneChan    chan struct{}
}

func newCursor() *cursor { return &cursor{doneChan: make(chan struct{})} }

func (c *cursor) Subscribe() *notifier {
	c.m.Lock()
	defer c.m.Unlock()

	n := newNotifier()
	c.subscribers = append(c.subscribers, n)
	return n
}

func (c *cursor) Unsubscribe(n *notifier) {
	c.m.Lock()
	defer c.m.Unlock()

	for i := range c.subscribers {
		if c.subscribers[i] == n {
			c.subscribers = append(c.subscribers[:i], c.subscribers[i+1:]...)
			break
		}
	}

	if len(c.subscribers) == 0 {
		select {
		case <-c.doneChan:
		default:
			close(c.doneChan)
		}
	}
}

// Done() returns a channel which gets closed when the number of
// subscribers drops to 0. If new subscribers get added after this, the
// channel remains closed.
func (c *cursor) Done() <-chan struct{} { return c.doneChan }

func (c *cursor) IsDone() bool {
	select {
	case <-c.doneChan:
		return true
	default:
		return false
	}
}

// SetPosition sets c.pos to the new value pos, but only if pos>c.pos. In the
// case that c.pos actually grew, all subscribers are notified.
func (c *cursor) SetPosition(pos int64) {
	if pos <= c.Position() {
		return
	}

	c.m.Lock()
	defer c.m.Unlock()

	// Check a second time now we hold the write lock.
	if pos <= c.pos {
		return
	}

	c.pos = pos
	for _, n := range c.subscribers {
		n.Notify()
	}
}

func (c *cursor) Position() int64 {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.pos
}

type notifier struct {
	C chan struct{} // The channel on which notifications are delivered
}

func newNotifier() *notifier { return &notifier{C: make(chan struct{}, 1)} }

func (n *notifier) Notify() {
	select {
	case n.C <- struct{}{}:
	default:
	}
}
