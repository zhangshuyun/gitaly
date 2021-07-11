package promtest

import (
	"sync"
)

// MockGauge is a mock gauge that adheres to prometheus.Gauge for use in unit tests
type MockGauge struct {
	m          sync.RWMutex
	Value      float64
	incs, decs int
}

// IncsCalled gives the number of times Inc() was been called
func (m *MockGauge) IncsCalled() int {
	m.m.RLock()
	defer m.m.RUnlock()
	return m.incs
}

// DecsCalled gives the number of times Inc() was been called
func (m *MockGauge) DecsCalled() int {
	m.m.RLock()
	defer m.m.RUnlock()
	return m.decs
}

// Inc increments the gauge value
func (m *MockGauge) Inc() {
	m.m.Lock()
	defer m.m.Unlock()
	m.Value++
	m.incs++
}

// Dec decrements the gauge value
func (m *MockGauge) Dec() {
	m.m.Lock()
	defer m.m.Unlock()
	m.Value--
	m.decs++
}

// MockStorageGauge wraps a MockGauge
type MockStorageGauge struct {
	*MockGauge
}

// Inc will track total calls to this method while ignoring params
func (m *MockStorageGauge) Inc(_, _ string) {
	m.MockGauge.Inc()
}

// Dec will track total calls to this method while ignoring params
func (m *MockStorageGauge) Dec(_, _ string) {
	m.MockGauge.Dec()
}
