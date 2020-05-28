package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
)

var (
	rpcTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_rpc_total",
			Help: "Total number of RPCs encountered by cache invalidator",
		},
	)
	rpcOpTypes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_optype_total",
			Help: "Total number of operation types encountered by cache invalidator",
		},
		[]string{"type"},
	)
	methodErrTotals = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_cacheinvalidator_error_total",
			Help: "Total number of cache invalidation errors by method",
		},
		[]string{"method"},
	)
)

// counter functions are package vars to allow for overriding in tests
var (
	countMethodErr = func(method string) { methodErrTotals.WithLabelValues(method).Inc() }
	countRPCType   = func(mInfo protoregistry.MethodInfo) {
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
)
