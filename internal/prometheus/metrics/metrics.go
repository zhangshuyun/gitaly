package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Counter is a subset of a prometheus Counter
type Counter interface {
	Inc()
	Add(float64)
}

// Gauge is a subset of a prometheus Gauge
type Gauge interface {
	Inc()
	Dec()
}

// Histogram is a subset of a prometheus Histogram
type Histogram interface {
	Observe(float64)
}

// HistogramVec is a subset of a prometheus HistogramVec
type HistogramVec interface {
	WithLabelValues(lvs ...string) prometheus.Observer
	Collect(chan<- prometheus.Metric)
	Describe(chan<- *prometheus.Desc)
}
