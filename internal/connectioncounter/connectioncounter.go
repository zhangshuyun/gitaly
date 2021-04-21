package connectioncounter

import (
	"net"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	connTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_connections_total",
			Help: "Total number of connections accepted by this Gitaly process",
		},
		[]string{"type"},
	)
)

// New returns a listener which increments a prometheus counter on each
// accepted connection. Use cType to specify the connection type, this is
// a prometheus label.
func New(cType string, l net.Listener) net.Listener {
	return &countingListener{
		cType:    cType,
		Listener: l,
	}
}

type countingListener struct {
	net.Listener
	cType string
}

func (cl *countingListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	connTotal.WithLabelValues(cl.cType).Inc()
	return conn, err
}
