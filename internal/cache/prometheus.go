package cache

import "github.com/prometheus/client_golang/prometheus"

var (
	requestTotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_requests_total",
			Help: "Total number of disk cache requests",
		},
	)
	missTotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_miss_total",
			Help: "Total number of disk cache misses",
		},
	)
	bytesStoredtotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_stored_total",
			Help: "Total number of disk cache bytes stored",
		},
	)
	bytesFetchedtotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_fetched_total",
			Help: "Total number of disk cache bytes fetched",
		},
	)
	bytesLoserTotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_loser_total",
			Help: "Total number of disk cache bytes from losing writes",
		},
	)
	errTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_errors_total",
			Help: "Total number of errors encountered by disk cache",
		},
		[]string{"error"},
	)
	walkerCheckTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_check_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
	walkerRemovalTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_removal_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
)

func init() {
	prometheus.MustRegister(requestTotals)
	prometheus.MustRegister(missTotals)
	prometheus.MustRegister(bytesStoredtotals)
	prometheus.MustRegister(bytesFetchedtotals)
	prometheus.MustRegister(bytesLoserTotals)
	prometheus.MustRegister(errTotal)
	prometheus.MustRegister(walkerCheckTotal)
	prometheus.MustRegister(walkerRemovalTotal)
}

func countErr(err error) error {
	switch err {
	case ErrMissingLeaseFile:
		errTotal.WithLabelValues("ErrMissingLeaseFile").Inc()
	case ErrPendingExists:
		errTotal.WithLabelValues("ErrPendingExists").Inc()
	}
	return err
}

var (
	countRequest     = func() { requestTotals.Inc() }
	countMiss        = func() { missTotals.Inc() }
	countWriteBytes  = func(n float64) { bytesStoredtotals.Add(n) }
	countReadBytes   = func(n float64) { bytesFetchedtotals.Add(n) }
	countLoserBytes  = func(n float64) { bytesLoserTotals.Add(n) }
	countWalkRemoval = func() { walkerRemovalTotal.Inc() }
	countWalkCheck   = func() { walkerCheckTotal.Inc() }
)
