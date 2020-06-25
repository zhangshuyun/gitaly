package tracker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrorTracker allows tracking how many read/write errors have occurred, and whether or not it has
// exceeded a configured threshold in a configured time window
type ErrorTracker interface {
	// IncrReadErr increases read errors by 1
	IncrReadErr(nodeStorage string)
	// IncrWriteErr increases write errors by 1
	IncrWriteErr(nodeStorage string)
	// ReadThresholdReached returns whether or not the read threshold was reached
	ReadThresholdReached(nodeStorage string) bool
	// WriteThresholdReached returns whether or not the read threshold was reached
	WriteThresholdReached(nodeStorage string) bool
}

type errorTracker struct {
	olderThan                     func() time.Time
	m                             sync.RWMutex
	writeThreshold, readThreshold int
	readErrors, writeErrors       map[string][]time.Time
	ctx                           context.Context
}

func newErrors(ctx context.Context, errorWindow time.Duration, readThreshold, writeThreshold uint32) (*errorTracker, error) {
	if errorWindow == 0 {
		return nil, errors.New("errorWindow must be non zero")
	}

	if readThreshold == 0 {
		return nil, errors.New("readThreshold must be non zero")
	}

	if writeThreshold == 0 {
		return nil, errors.New("writeThreshold must be non zero")
	}

	e := &errorTracker{
		olderThan: func() time.Time {
			return time.Now().Add(-errorWindow)
		},
		readErrors:     make(map[string][]time.Time),
		writeErrors:    make(map[string][]time.Time),
		readThreshold:  int(readThreshold),
		writeThreshold: int(writeThreshold),
		ctx:            ctx,
	}
	go e.periodicallyClear()

	return e, nil
}

// NewErrors creates a new Error instance given a time window in seconds, and read and write thresholds
func NewErrors(ctx context.Context, errorWindow time.Duration, readThreshold, writeThreshold uint32) (ErrorTracker, error) {
	return newErrors(ctx, errorWindow, readThreshold, writeThreshold)
}

// IncrReadErr increases the read errors for a node by 1
func (e *errorTracker) IncrReadErr(node string) {
	select {
	case <-e.ctx.Done():
		return
	default:
		e.m.Lock()
		defer e.m.Unlock()

		e.readErrors[node] = append(e.readErrors[node], time.Now())

		if len(e.readErrors[node]) > e.readThreshold {
			e.readErrors[node] = e.readErrors[node][1:]
		}
	}
}

// IncrWriteErr increases the read errors for a node by 1
func (e *errorTracker) IncrWriteErr(node string) {
	select {
	case <-e.ctx.Done():
		return
	default:
		e.m.Lock()
		defer e.m.Unlock()

		e.writeErrors[node] = append(e.writeErrors[node], time.Now())

		if len(e.writeErrors[node]) > e.writeThreshold {
			e.writeErrors[node] = e.writeErrors[node][1:]
		}
	}
}

// ReadThresholdReached indicates whether or not the read threshold has been reached within the time window
func (e *errorTracker) ReadThresholdReached(node string) bool {
	select {
	case <-e.ctx.Done():
		break
	default:
		e.m.RLock()
		defer e.m.RUnlock()

		olderThanTime := e.olderThan()

		for i, errTime := range e.readErrors[node] {
			if errTime.After(olderThanTime) {
				if i == 0 {
					return len(e.readErrors[node]) >= e.readThreshold
				}
				return len(e.readErrors[node][i-1:]) >= e.readThreshold
			}
		}
	}

	return false
}

// WriteThresholdReached indicates whether or not the write threshold has been reached within the time window
func (e *errorTracker) WriteThresholdReached(node string) bool {
	select {
	case <-e.ctx.Done():
		break
	default:
		e.m.RLock()
		defer e.m.RUnlock()

		olderThanTime := e.olderThan()

		for i, errTime := range e.writeErrors[node] {
			if errTime.After(olderThanTime) {
				if i == 0 {
					return len(e.writeErrors[node]) >= e.writeThreshold
				}
				return len(e.writeErrors[node][i-1:]) >= e.writeThreshold
			}
		}
	}

	return false
}

// periodicallyClear runs in an infinite loop clearing out old error entries
func (e *errorTracker) periodicallyClear() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			e.clear()
		case <-e.ctx.Done():
			e.m.Lock()
			defer e.m.Unlock()
			e.readErrors = nil
			e.writeErrors = nil
			return
		}
	}
}

func (e *errorTracker) clear() {
	e.m.Lock()
	defer e.m.Unlock()

	olderThanTime := e.olderThan()

	clearErrors(e.writeErrors, olderThanTime)
	clearErrors(e.readErrors, olderThanTime)
}

func clearErrors(errs map[string][]time.Time, olderThan time.Time) {
	for node, errors := range errs {
		for i, errTime := range errors {
			if errTime.After(olderThan) {
				errs[node] = errors[i:]
				break
			}

			if i == len(errors)-1 {
				errs[node] = errs[node][:0]
			}
		}
	}
}
