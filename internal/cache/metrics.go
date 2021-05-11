package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestTotals = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_requests_total",
			Help: "Total number of disk cache requests",
		},
	)
	missTotals = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_miss_total",
			Help: "Total number of disk cache misses",
		},
	)
	bytesStoredtotals = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_stored_total",
			Help: "Total number of disk cache bytes stored",
		},
	)
	bytesFetchedtotals = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_fetched_total",
			Help: "Total number of disk cache bytes fetched",
		},
	)
	bytesLoserTotals = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_bytes_loser_total",
			Help: "Total number of disk cache bytes from losing writes",
		},
	)
	errTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_errors_total",
			Help: "Total number of errors encountered by disk cache",
		},
		[]string{"error"},
	)
	walkerCheckTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_check_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
	walkerRemovalTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_removal_total",
			Help: "Total number of events during diskcache filesystem walks",
		},
	)
	walkerErrorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_error_total",
			Help: "Total number of errors during diskcache filesystem walks",
		},
	)
	walkerEmptyDirTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_empty_dir_total",
			Help: "Total number of empty directories encountered",
		},
	)
	walkerEmptyDirRemovalTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_diskcache_walker_empty_dir_removal_total",
			Help: "Total number of empty directories removed",
		},
	)
)

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
	countRequest         = func() { requestTotals.Inc() }
	countMiss            = func() { missTotals.Inc() }
	countWriteBytes      = func(n float64) { bytesStoredtotals.Add(n) }
	countReadBytes       = func(n float64) { bytesFetchedtotals.Add(n) }
	countLoserBytes      = func(n float64) { bytesLoserTotals.Add(n) }
	countWalkRemoval     = func() { walkerRemovalTotal.Inc() }
	countWalkCheck       = func() { walkerCheckTotal.Inc() }
	countWalkError       = func() { walkerErrorTotal.Inc() }
	countEmptyDir        = func() { walkerEmptyDirTotal.Inc() }
	countEmptyDirRemoval = func() { walkerEmptyDirRemovalTotal.Inc() }
)
