package rubyserver

import (
	"fmt"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver/balancer"
	"gitlab.com/gitlab-org/gitaly/v14/internal/supervisor"
)

var (
	terminationCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_ruby_memory_terminations_total",
			Help: "Number of times gitaly-ruby has been terminated because of excessive memory use.",
		},
		[]string{"name"},
	)
)

// worker observes the event stream of a supervised process and restarts
// it if necessary, in cooperation with the balancer.
type worker struct {
	*supervisor.Process
	address                string
	restartDelay           time.Duration
	gracefulRestartTimeout time.Duration
	events                 <-chan supervisor.Event
	shutdown               chan struct{}
	monitorDone            chan struct{}

	// This is for testing only, so that we can inject a fake balancer
	balancerUpdate chan balancerProxy

	testing bool
}

func newWorker(p *supervisor.Process, address string, restartDelay, gracefulRestartTimeout time.Duration, events <-chan supervisor.Event, testing bool) *worker {
	w := &worker{
		Process:                p,
		address:                address,
		restartDelay:           restartDelay,
		gracefulRestartTimeout: gracefulRestartTimeout,
		events:                 events,
		shutdown:               make(chan struct{}),
		monitorDone:            make(chan struct{}),
		balancerUpdate:         make(chan balancerProxy),
		testing:                testing,
	}
	go w.monitor()

	bal := defaultBalancer{}
	w.balancerUpdate <- bal

	// When we return from this function, requests may start coming in. If
	// there are no addresses in the balancer when the first request comes in
	// we can get a panic from grpc-go. So before returning, we ensure the
	// current address has been added to the balancer.
	bal.AddAddress(w.address)

	return w
}

type balancerProxy interface {
	AddAddress(string)
	RemoveAddress(string) bool
}

type defaultBalancer struct{}

func (defaultBalancer) AddAddress(s string)         { balancer.AddAddress(s) }
func (defaultBalancer) RemoveAddress(s string) bool { return balancer.RemoveAddress(s) }

var (
	// Ignore health checks for the current process after it just restarted
	healthRestartCoolOff = 5 * time.Minute
	// Health considered bad after sustained failed health checks
	healthRestartDelay = 1 * time.Minute
)

func (w *worker) monitor() {
	swMem := &stopwatch{}
	swHealth := &stopwatch{}
	lastRestart := time.Now()
	currentPid := 0
	bal := <-w.balancerUpdate

	for {
	nextEvent:
		select {
		case e := <-w.events:
			switch e.Type {
			case supervisor.Up:
				if badPid(e.Pid) {
					w.logBadEvent(e)
					break nextEvent
				}

				if e.Pid == currentPid {
					// Ignore repeated events to avoid constantly resetting our internal
					// state.
					break nextEvent
				}

				bal.AddAddress(w.address)
				currentPid = e.Pid

				swMem.reset()
				swHealth.reset()
				lastRestart = time.Now()
			case supervisor.MemoryHigh:
				if badPid(e.Pid) {
					w.logBadEvent(e)
					break nextEvent
				}

				if e.Pid != currentPid {
					break nextEvent
				}

				swMem.mark()
				if swMem.elapsed() <= w.restartDelay {
					break nextEvent
				}

				// It is crucial to check the return value of RemoveAddress. If we don't
				// we may leave the system without the capacity to make gitaly-ruby
				// requests.
				if bal.RemoveAddress(w.address) {
					w.logPid(currentPid).Info("removed gitaly-ruby worker from balancer due to high memory")
					go w.waitTerminate(currentPid)
					swMem.reset()
				}
			case supervisor.MemoryLow:
				if badPid(e.Pid) {
					w.logBadEvent(e)
					break nextEvent
				}

				if e.Pid != currentPid {
					break nextEvent
				}

				swMem.reset()
			case supervisor.HealthOK:
				swHealth.reset()
			case supervisor.HealthBad:
				if time.Since(lastRestart) <= healthRestartCoolOff {
					// Ignore health checks for a while after the supervised process restarted
					break nextEvent
				}

				w.log().WithError(e.Error).Warn("gitaly-ruby worker health check failed")

				swHealth.mark()
				if swHealth.elapsed() <= healthRestartDelay {
					break nextEvent
				}

				if bal.RemoveAddress(w.address) {
					w.logPid(currentPid).Info("removed gitaly-ruby worker from balancer due to sustained failing health checks")
					go w.waitTerminate(currentPid)
					swHealth.reset()
				}
			default:
				panic(fmt.Sprintf("unknown state %v", e.Type))
			}
		case bal = <-w.balancerUpdate:
			// For testing only.
		case <-w.shutdown:
			close(w.monitorDone)
			return
		}
	}
}

func (w *worker) stopMonitor() {
	close(w.shutdown)
	<-w.monitorDone
}

func badPid(pid int) bool {
	return pid <= 0
}

func (w *worker) log() *log.Entry {
	return log.WithFields(log.Fields{
		"worker.name": w.Name,
	})
}

func (w *worker) logPid(pid int) *log.Entry {
	return w.log().WithFields(log.Fields{
		"worker.pid": pid,
	})
}

func (w *worker) logBadEvent(e supervisor.Event) {
	w.log().WithFields(log.Fields{
		"worker.event": e,
	}).Error("monitor state machine received bad event")
}

func (w *worker) waitTerminate(pid int) {
	if w.testing {
		return
	}

	// Wait for in-flight requests to reach the worker before we slam the
	// door in their face.
	time.Sleep(1 * time.Minute)

	terminationCounter.WithLabelValues(w.Name).Inc()

	w.logPid(pid).Info("sending SIGTERM")
	syscall.Kill(pid, syscall.SIGTERM)

	time.Sleep(w.gracefulRestartTimeout)

	w.logPid(pid).Info("sending SIGKILL")
	syscall.Kill(pid, syscall.SIGKILL)
}
