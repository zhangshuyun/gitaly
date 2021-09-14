package sidechannel

import (
	"fmt"
	"net"
	"sync"
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
	errC     chan error
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
		errC:     make(chan error),
		accept:   make(chan net.Conn),
		callback: callback,
	}
	s.nextID++

	go waiter.run()
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

func (w *Waiter) run() {
	defer close(w.errC)

	if conn := <-w.accept; conn != nil {
		defer conn.Close()
		w.errC <- w.callback(newClientConn(conn))
	}
}

// Close cleans the waiter, removes it from the registry. If the callback is
// executing, this method is blocked until the callback is done.
func (w *Waiter) Close() error {
	w.registry.removeWaiter(w)
	return <-w.errC
}

// Wait waits until either the callback is executed, or the waiter is closed
func (w *Waiter) Wait() error {
	return <-w.errC
}
