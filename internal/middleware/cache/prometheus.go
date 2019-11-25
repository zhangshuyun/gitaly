package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
)

var (
	rpcTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_rpc_total",
			Help: "Total number of RPCs encountered by cache invalidator",
		},
	)
	rpcOpTypes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_optype_total",
			Help: "Total number of operation types encountered by cache invalidator",
		},
		[]string{"type"},
	)
	methodErrTotals = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_error_total",
			Help: "Total number of cache invalidation errors by method",
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(rpcTotal)
	prometheus.MustRegister(rpcOpTypes)
	prometheus.MustRegister(methodErrTotals)
}

// counter functions are package vars to allow for overriding in tests
var (
	countMethodErr = func(method string) { methodErrTotals.WithLabelValues(method).Inc() }
)

func countRPCType(mInfo protoregistry.MethodInfo) {
	rpcTotal.Inc()

	switch mInfo.Operation {
	case protoregistry.OpAccessor:
		rpcOpTypes.WithLabelValues("accessor").Inc()
	case protoregistry.OpMutator:
		rpcOpTypes.WithLabelValues("mutator").Inc()
	default:
		rpcOpTypes.WithLabelValues("unknown").Inc()
	}
}
