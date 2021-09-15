package sidechannel

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// sidechannelID is the type of ID used to differeniate sidechannel connections
// in the same registry
type sidechannelID int64

// Registry manages sidechannel connections. It allows the RPC
// handlers to wait for the secondary incoming connection made by the client.
type Registry struct {
	nextID  sidechannelID
	waiters map[sidechannelID]*Waiter
	mu      sync.Mutex
}

// Waiter lets the caller waits until a connection with matched id is pushed
// into the registry, then execute the callback
type Waiter struct {
	id       sidechannelID
	registry *Registry
	err      error
	done     chan struct{}
	accept   chan net.Conn
	callback func(*ClientConn) error
}

// NewRegistry returns a new Registry instance
func NewRegistry() *Registry {
	return &Registry{
		waiters: make(map[sidechannelID]*Waiter),
	}
}

// Register registers the caller into the waiting list. The caller must provide
// a callback function. The caller receives a waiter instance.  After the
// connection arrives, the callback function is executed with arrived
// connection in a new goroutine.  The caller receives execution result via
// waiter.Wait().
func (s *Registry) Register(callback func(*ClientConn) error) *Waiter {
	s.mu.Lock()
	defer s.mu.Unlock()

	waiter := &Waiter{
		id:       s.nextID,
		registry: s,
		done:     make(chan struct{}),
		accept:   make(chan net.Conn),
		callback: callback,
	}
	s.nextID++

	go func() {
		defer close(waiter.done)
		waiter.err = waiter.run()
	}()
	s.waiters[waiter.id] = waiter
	return waiter
}

// receive looks into the registry for a waiter with the given ID. If
// there is an associated ID, the waiter is removed from the registry, and the
// connection is pushed into the waiter's accept channel. After the callback is done, the
// connection is closed. When the ID is not found, an error is returned and the
// connection is closed immediately.
func (s *Registry) receive(id sidechannelID, conn net.Conn) (err error) {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		if err != nil {
			conn.Close()
		}
	}()

	waiter, exist := s.waiters[id]
	if !exist {
		return fmt.Errorf("sidechannel registry: ID not registered")
	}
	delete(s.waiters, waiter.id)
	waiter.accept <- conn

	return nil
}

func (s *Registry) removeWaiter(waiter *Waiter) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exist := s.waiters[waiter.id]; exist {
		delete(s.waiters, waiter.id)
		close(waiter.accept)
	}
}

func (s *Registry) waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.waiters)
}

// ErrCallbackDidNotRun indicates that a sidechannel callback was
// de-registered without having run. This can happen if the server chose
// not to use the sidechannel.
var ErrCallbackDidNotRun = errors.New("sidechannel: callback de-registered without having run")

func (w *Waiter) run() error {
	conn := <-w.accept
	if conn == nil {
		return ErrCallbackDidNotRun
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(sidechannelTimeout)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte("ok")); err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}

	return w.callback(newClientConn(conn))
}

// Close de-registers the callback. If the callback got triggered,
// Close() will return its error return value. If the callback has not
// started by the time Close() is called, Close() returns
// ErrCallbackDidNotRun.
func (w *Waiter) Close() error {
	w.registry.removeWaiter(w)
	<-w.done
	return w.err
}
