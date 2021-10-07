// Package dontpanic provides function wrappers and supervisors to ensure
// that wrapped code does not panic and cause program crashes.
//
// When should you use this package? Anytime you are running a function or
// goroutine where it isn't obvious whether it can or can't panic. This may
// be a higher risk in long running goroutines and functions or ones that are
// difficult to test completely.
package dontpanic

import (
	"sync"
	"time"

	sentry "github.com/getsentry/sentry-go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

// Try will wrap the provided function with a panic recovery. If a panic occurs,
// the recovered panic will be sent to Sentry and logged as an error.
// Returns `true` if no panic and `false` otherwise.
func Try(fn func()) bool { return catchAndLog(fn) }

// Go will run the provided function in a goroutine and recover from any
// panics.  If a panic occurs, the recovered panic will be sent to Sentry
// and logged as an error. Go is best used in fire-and-forget goroutines where
// observability is lost.
func Go(fn func()) { go Try(fn) }

var logger = log.Default()

func catchAndLog(fn func()) bool {
	var id *sentry.EventID
	var recovered interface{}
	normal := true

	func() {
		defer func() {
			recovered = recover()
			if recovered != nil {
				normal = false
			}

			if err, ok := recovered.(error); ok {
				id = sentry.CaptureException(err)
			}
		}()
		fn()
	}()

	if id == nil || *id == "" {
		return normal
	}

	logger.WithField("sentry_id", id).Errorf(
		"dontpanic: recovered value sent to Sentry: %+v", recovered,
	)
	return normal
}

// Forever encapsulates logic to run a function forever.
type Forever struct {
	backoff time.Duration

	cancelOnce sync.Once
	cancelCh   chan struct{}
	doneCh     chan struct{}
}

// NewForever creates a new Forever struct. The given duration controls how long retry of a
// function should be delayed if the function were to thrown an error.
func NewForever(backoff time.Duration) *Forever {
	return &Forever{
		backoff:  backoff,
		cancelCh: make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Go will keep retrying a function fn in a goroutine forever in the background (until the process
// exits) while recovering from panics. Each time a closure panics, the recovered value will be
// sent to Sentry and logged at level error. The provided backoff will delay retries to enable
// "breathing" room to prevent potentially worsening the situation.
func (f *Forever) Go(fn func()) {
	go func() {
		defer close(f.doneCh)

		for {
			select {
			case <-f.cancelCh:
				return
			default:
			}

			if Try(fn) {
				continue
			}

			if f.backoff <= 0 {
				continue
			}

			logger.Infof("dontpanic: backing off %s before retrying", f.backoff)

			select {
			case <-f.cancelCh:
				return
			case <-time.After(f.backoff):
			}
		}
	}()
}

// Cancel cancels the walking loop.
func (f *Forever) Cancel() {
	f.cancelOnce.Do(func() {
		close(f.cancelCh)
		<-f.doneCh
	})
}
