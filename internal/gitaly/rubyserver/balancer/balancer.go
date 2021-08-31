package balancer

// In this package we manage a global pool of addresses for gitaly-ruby,
// accessed via the gitaly-ruby:// scheme. The interface consists of the
// AddAddress and RemoveAddress methods. RemoveAddress returns a boolean
// indicating whether the address was removed; this is intended to give
// back-pressure against repeated process restarts.
//
// The gitaly-ruby:// scheme exists because that is the way we can
// interact with the internal client-side loadbalancer of grpc-go. A URL
// for this scheme would be gitaly-ruby://foobar. For gitaly-ruby://
// URL's, the host and port are ignored. So gitaly-ruby://foobar is
// actually a working, valid address.
//
// Strictly speaking this package implements a gRPC 'Resolver'. This
// resolver feeds address list updates to a gRPC 'balancer' which
// interacts with the gRPC client connection machinery. A resolver
// consists of a Builder which returns Resolver instances. Our Builder
// manages the address pool and notifies its Resolver instances of
// changes, which they then propagate into the gRPC library.
//

import (
	"time"

	"google.golang.org/grpc/resolver"
)

var lbBuilder = newBuilder()

func init() {
	resolver.Register(lbBuilder)
}

const (
	// DefaultRemoveDelay is the minimum time between successive address removals.
	DefaultRemoveDelay = 1 * time.Minute
)

// AddAddress adds the address of a gitaly-ruby instance to the load
// balancer.
func AddAddress(a string) {
	done := make(chan struct{})
	lbBuilder.addAddress <- addressAddition{addr: a, done: done}
	<-done
}

// RemoveAddress removes the address of a gitaly-ruby instance from the
// load balancer. Returns false if the pool is too small to remove the
// address.
func RemoveAddress(addr string) bool {
	ok := make(chan bool)
	lbBuilder.removeAddress <- addressRemoval{ok: ok, addr: addr}
	return <-ok
}

type addressAddition struct {
	addr string
	done chan<- struct{}
}

type addressRemoval struct {
	addr string
	ok   chan<- bool
}

type addressUpdate struct {
	addrs []resolver.Address
	next  chan struct{}
}

// NowFunc returns the current time.
type NowFunc func() time.Time

type config struct {
	numAddrs    int
	removeDelay time.Duration
	now         NowFunc
}

type builder struct {
	addAddress     chan addressAddition
	removeAddress  chan addressRemoval
	addressUpdates chan addressUpdate
	configUpdate   chan config

	// testingTriggerRestart is for testing only. It causes b.monitor(...) to
	// re-execute.
	testingTriggerRestart chan struct{}
}

// ConfigureBuilder changes the configuration of the global balancer
// instance. All calls that interact with the balancer will block until
// ConfigureBuilder has been called at least once.
func ConfigureBuilder(numAddrs int, removeDelay time.Duration, now NowFunc) {
	cfg := config{
		numAddrs:    numAddrs,
		removeDelay: removeDelay,
		now:         now,
	}

	if cfg.removeDelay <= 0 {
		cfg.removeDelay = DefaultRemoveDelay
	}
	if numAddrs <= 0 {
		panic("numAddrs must be at least 1")
	}

	lbBuilder.configUpdate <- cfg
}

func newBuilder() *builder {
	b := &builder{
		addAddress:            make(chan addressAddition),
		removeAddress:         make(chan addressRemoval),
		addressUpdates:        make(chan addressUpdate),
		configUpdate:          make(chan config),
		testingTriggerRestart: make(chan struct{}),
	}
	go b.monitor()

	return b
}

// Scheme is the name of the address scheme that makes gRPC select this resolver.
const Scheme = "gitaly-ruby"

func (*builder) Scheme() string { return Scheme }

// Build ignores its resolver.Target argument. That means it does not
// care what "address" the caller wants to resolve. We always resolve to
// the current list of address for local gitaly-ruby processes.
func (b *builder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	//nolint:staticcheck // There is no obvious way to use UpdateState() without completely replacing the current configuration
	cc.NewServiceConfig(`{"LoadBalancingPolicy":"round_robin"}`)
	return newGitalyResolver(cc, b.addressUpdates), nil
}

// monitor serves address list requests and handles address updates.
func (b *builder) monitor() {
	p := newPool()
	notify := make(chan struct{})
	cfg := <-b.configUpdate

	// At this point, there has been no previous removal command yet, so the
	// "last removal" is undefined. We want it to default to "long enough
	// ago".
	var lastRemoval time.Time

	// This channel is intentionally nil so that our 'select' below won't
	// send messages to it. We do this to prevent sending out invalid (empty)
	// messages during boot.
	var addressUpdates chan addressUpdate

	for {
		au := addressUpdate{next: notify}
		for _, a := range p.activeAddrs() {
			au.addrs = append(au.addrs, resolver.Address{Addr: a})
		}

		if len(au.addrs) > 0 && addressUpdates == nil {
			// Start listening for address update requests
			addressUpdates = b.addressUpdates
		}

		select {
		case addressUpdates <- au:
			// We have served an address update request
		case addition := <-b.addAddress:
			p.add(addition.addr)
			close(addition.done)

			notify = broadcast(notify)
		case removal := <-b.removeAddress:
			now := cfg.now()
			if now.Sub(lastRemoval) < cfg.removeDelay || p.activeSize() < cfg.numAddrs-1 {
				removal.ok <- false
				break
			}

			if !p.remove(removal.addr) {
				removal.ok <- false
				break
			}

			removal.ok <- true
			lastRemoval = now
			notify = broadcast(notify)
		case cfg = <-b.configUpdate:
			// We have received a config update
		case <-b.testingTriggerRestart:
			go b.monitor()
			b.configUpdate <- cfg
			return
		}
	}
}

// broadcast returns a fresh channel because we can only close them once
func broadcast(ch chan struct{}) chan struct{} {
	close(ch)
	return make(chan struct{})
}

// gitalyResolver propagates address list updates to a
// resolver.ClientConn instance
type gitalyResolver struct {
	clientConn resolver.ClientConn

	started        chan struct{}
	done           chan struct{}
	resolveNow     chan struct{}
	addressUpdates chan addressUpdate
}

func newGitalyResolver(cc resolver.ClientConn, auCh chan addressUpdate) *gitalyResolver {
	r := &gitalyResolver{
		started:        make(chan struct{}),
		done:           make(chan struct{}),
		resolveNow:     make(chan struct{}),
		addressUpdates: auCh,
		clientConn:     cc,
	}
	go r.monitor()

	// Don't return until we have sent at least one address update. This is
	// meant to avoid panics inside the grpc-go library.
	<-r.started

	return r
}

func (r *gitalyResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.resolveNow <- struct{}{}
}

func (r *gitalyResolver) Close() {
	close(r.done)
}

func (r *gitalyResolver) monitor() {
	notify := r.sendUpdate()
	close(r.started)

	for {
		select {
		case <-notify:
			notify = r.sendUpdate()
		case <-r.resolveNow:
			notify = r.sendUpdate()
		case <-r.done:
			return
		}
	}
}

func (r *gitalyResolver) sendUpdate() chan struct{} {
	au := <-r.addressUpdates
	//nolint:staticcheck // There is no obvious way to use UpdateState() without completely replacing the current configuration
	r.clientConn.NewAddress(au.addrs)
	return au.next
}
