package promtest

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// MockHistogram is a mock histogram that adheres to prometheus.Histogram for use in unit tests
type MockHistogram struct {
	m      sync.RWMutex
	Values []float64
}

// Observe observes a value for the mock histogram
func (m *MockHistogram) Observe(v float64) {
	m.m.Lock()
	defer m.m.Unlock()
	m.Values = append(m.Values, v)
}

// MockHistogramVec implements a subset of the prometheus.HistogramVec interface.
type MockHistogramVec struct {
	m            sync.RWMutex
	labelsCalled [][]string
	observer     MockObserver
}

// NewMockHistogramVec returns a new MockHistogramVec.
func NewMockHistogramVec() *MockHistogramVec {
	return &MockHistogramVec{}
}

// LabelsCalled returns the set of labels which have been observed.
func (m *MockHistogramVec) LabelsCalled() [][]string {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.labelsCalled
}

// Observer returns the mocked observer.
func (m *MockHistogramVec) Observer() *MockObserver {
	return &m.observer
}

// Collect does nothing.
func (m *MockHistogramVec) Collect(chan<- prometheus.Metric) {}

// Describe does nothing.
func (m *MockHistogramVec) Describe(chan<- *prometheus.Desc) {}

// WithLabelValues records the given labels such that `LabelsCalled()` will return the set of
// observed labels.
func (m *MockHistogramVec) WithLabelValues(lvs ...string) prometheus.Observer {
	m.m.Lock()
	defer m.m.Unlock()

	m.labelsCalled = append(m.labelsCalled, lvs)
	return &m.observer
}

// MockObserver implements a subset of the prometheus.Observer interface.
type MockObserver struct {
	m        sync.RWMutex
	observed []float64
}

// Observe records the given value in its observed values.
func (m *MockObserver) Observe(v float64) {
	m.m.Lock()
	defer m.m.Unlock()

	m.observed = append(m.observed, v)
}

// Observed returns all observed values.
func (m *MockObserver) Observed() []float64 {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.observed
}
