package streamrpcs

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// globalRegistry is a single per-process registry. The RPC handlers are
// expected to use this registry to wait for incoming sidechannel connection
var globalRegistry *Registry

func init() {
	globalRegistry = NewRegistry()
}

// Registry manages StreamRPC sidechannel connections. It allows the RPC
// handlers to wait for the secondary incoming connection made by the client.
// In details:
// - We'd add additional RPCs (PostUploadPackStream, PackObjectsHookStream,
// SSHUploadPackStream) to return a streaming token to the client once the
// verifications are done and they are ready to launch git to begin streaming
// the response.
// - The RPC handler waits for StreamRPC connection registry
// - Client receives the streaming token. It then dials to the listening port of Gitaly server.
// - Listenmux handles the connection. It validates the connection, and pushes
// to StreamRPC connection registry; then exists without error.
// - The RPC handler receives the connection once it's accepted, and passes it to git.
// - Git runs and streams directly to the client over the TCP.
// - Once git returns, the gRPC handler returns normally with success/error to the client.
type Registry struct {
	waiters map[string]*Waiter
	stopped bool
	mu      sync.Mutex
}

// Waiter lets the caller waits until a connection with matched token is pushed
// into the registry.
type Waiter struct {
	Token   string
	err     error
	c       chan net.Conn
	timeout *time.Timer
}

// NewRegistry returns a new Registry instance
func NewRegistry() *Registry {
	return &Registry{
		waiters: make(map[string]*Waiter),
	}
}

// Register registers the caller into the waiting list. The caller must provide
// a deadline for this operation. The caller receives a Waiter struct with
// associated token. The caller is expected to be blocked by waiter.Wait().
// After the connection arrives, or the deadline exceeds, the waiter struct is
// removed from the registry automatically and the caller is unblocked.
func (s *Registry) Register(deadline time.Time) (*Waiter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return nil, fmt.Errorf("stream rpc registry: register already stopped")
	}

	waiter := &Waiter{
		Token: s.generateToken(),
		c:     make(chan net.Conn, 1),
	}
	waiter.timeout = time.AfterFunc(time.Until(deadline), func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.removeWaiter(waiter, fmt.Errorf("stream rpc registry: timeout exceeds"))
	})

	s.waiters[waiter.Token] = waiter
	return waiter, nil
}

// Push pushes a connection with an pre-registered token into the registry. The
// caller is unlocked immediately, and the waiter is removed from the registry.
// If there isn't any waiting caller, this function still exists, the caller
// can pulls the connection later through waiter struct.
func (s *Registry) Push(token string, conn net.Conn) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return fmt.Errorf("stream rpc registry: register already stopped")
	}

	var waiter *Waiter
	var exist bool
	if waiter, exist = s.waiters[token]; !exist {
		return fmt.Errorf("stream rpc registry: connection not registered")
	}

	waiter.c <- conn
	s.removeWaiter(waiter, nil)

	return nil
}

// Stop immedicately removes all waiters from the registry and prevent any
// Register/Push operations in the future.
func (s *Registry) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stopped = true
	for _, waiter := range s.waiters {
		s.removeWaiter(waiter, fmt.Errorf("stream rpc registry: register already stopped"))
	}
}

// Waiting returns the number of recent waiters the register is managing
func (s *Registry) Waiting() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.waiters)
}

func (s *Registry) removeWaiter(waiter *Waiter, err error) {
	if err != nil {
		waiter.err = err
	}
	waiter.timeout.Stop()
	close(waiter.c)
	delete(s.waiters, waiter.Token)
}

// TokenSizeBytes indicates the size of stringify token. In this case, UUID
// consists of 32 characters and 4 dashes
const TokenSizeBytes = 36

// generateToken generates a unique token to be used as the hash key for
// waiter. UUID is a good choice to generate a random unique token. It's size
// is deterministic, well randomlized, and fairly distributed.
func (s *Registry) generateToken() string {
	for {
		token := uuid.New().String()
		if _, exist := s.waiters[token]; !exist {
			return token
		}
	}
}

// Wait blocks the caller until a matched connection arrives, or waiter
// deadline exceeds, or the registry stops.
func (waiter *Waiter) Wait() (net.Conn, error) {
	return <-waiter.c, waiter.err
}
