package starter

import (
	"net"

	"github.com/prometheus/client_golang/prometheus"
)

// wrap returns a listener which increments a prometheus counter on each
// accepted connection. Use cType to specify the connection type, this is
// a prometheus label.
func wrap(cType string, l net.Listener, counter *prometheus.CounterVec) net.Listener {
	return &countingListener{
		cType:    cType,
		Listener: l,
		counter:  counter,
	}
}

type countingListener struct {
	net.Listener
	cType   string
	counter *prometheus.CounterVec
}

func (cl *countingListener) Accept() (net.Conn, error) {
	conn, err := cl.Listener.Accept()
	cl.counter.WithLabelValues(cl.cType).Inc()
	return conn, err
}
