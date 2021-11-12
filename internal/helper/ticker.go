package helper

import "time"

// Ticker ticks on the channel returned by C to signal something.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset()
}

// NewTimerTicker returns a Ticker that ticks after the specified interval
// has passed since the previous Reset call.
func NewTimerTicker(interval time.Duration) Ticker {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	return &timerTicker{timer: timer, interval: interval}
}

type timerTicker struct {
	timer    *time.Timer
	interval time.Duration
}

func (tt *timerTicker) C() <-chan time.Time { return tt.timer.C }

// Reset resets the timer. If there is a pending tick, then this tick will be drained.
func (tt *timerTicker) Reset() {
	if !tt.timer.Stop() {
		select {
		case <-tt.timer.C:
		default:
		}
	}
	tt.timer.Reset(tt.interval)
}

func (tt *timerTicker) Stop() { tt.timer.Stop() }

// ManualTicker implements a ticker that ticks when Tick is called.
// Stop and Reset functions call the provided functions.
type ManualTicker struct {
	c         chan time.Time
	StopFunc  func()
	ResetFunc func()
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (mt *ManualTicker) C() <-chan time.Time { return mt.c }

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (mt *ManualTicker) Stop() { mt.StopFunc() }

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (mt *ManualTicker) Reset() { mt.ResetFunc() }

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (mt *ManualTicker) Tick() { mt.c <- time.Now() }

// NewManualTicker returns a Ticker that can be manually controlled.
func NewManualTicker() *ManualTicker {
	return &ManualTicker{
		c:         make(chan time.Time, 1),
		StopFunc:  func() {},
		ResetFunc: func() {},
	}
}

// NewCountTicker returns a ManualTicker with a ResetFunc that
// calls the provided callback on Reset call after it has been
// called N times.
func NewCountTicker(n int, callback func()) *ManualTicker {
	ticker := NewManualTicker()
	ticker.ResetFunc = func() {
		n--
		if n < 0 {
			callback()
			return
		}

		ticker.Tick()
	}

	return ticker
}
