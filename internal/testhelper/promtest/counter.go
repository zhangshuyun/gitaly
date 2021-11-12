package promtest

import (
	"sync"
)

//nolint: revive,stylecheck // This is unintentionally missing documentation.
type MockCounter struct {
	m     sync.RWMutex
	value float64
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m *MockCounter) Value() float64 {
	m.m.RLock()
	defer m.m.RUnlock()
	return m.value
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m *MockCounter) Inc() {
	m.Add(1)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (m *MockCounter) Add(v float64) {
	m.m.Lock()
	defer m.m.Unlock()
	m.value += v
}
