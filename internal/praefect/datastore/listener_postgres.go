package datastore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// PostgresListenerOpts is a set of configuration options for the PostgreSQL listener.
type PostgresListenerOpts struct {
	// Addr is an address to database instance.
	Addr string
	// Channels is a list of channel to listen for notifications.
	Channels []string
	// PingPeriod is a period to wait before executing a pin call on the connection to verify if it is still healthy.
	PingPeriod time.Duration
	// MinReconnectInterval controls the duration to wait before trying to
	// re-establish the database connection after connection loss.
	MinReconnectInterval time.Duration
	// MaxReconnectInterval is a max interval to wait until successful connection establishment.
	MaxReconnectInterval time.Duration
}

// DefaultPostgresListenerOpts pre-defined options for PostgreSQL listener.
var DefaultPostgresListenerOpts = PostgresListenerOpts{
	PingPeriod:           10 * time.Second,
	MinReconnectInterval: 5 * time.Second,
	MaxReconnectInterval: 40 * time.Second,
}

// PostgresListener is an implementation based on the PostgreSQL LISTEN/NOTIFY functions.
type PostgresListener struct {
	logger         logrus.FieldLogger
	listener       *pq.Listener
	handler        glsql.ListenHandler
	opts           PostgresListenerOpts
	closed         chan struct{}
	reconnectTotal *promclient.CounterVec
	wg             sync.WaitGroup
}

// NewPostgresListener returns a new instance of the listener.
func NewPostgresListener(logger logrus.FieldLogger, opts PostgresListenerOpts, handler glsql.ListenHandler) (*PostgresListener, error) {
	switch {
	case strings.TrimSpace(opts.Addr) == "":
		return nil, fmt.Errorf("address is invalid: %q", opts.Addr)
	case len(opts.Channels) == 0:
		return nil, errors.New("no channels to listen")
	case opts.PingPeriod < 0:
		return nil, fmt.Errorf("invalid ping period: %s", opts.PingPeriod)
	case opts.MinReconnectInterval <= 0:
		return nil, fmt.Errorf("invalid min reconnect period: %s", opts.MinReconnectInterval)
	case opts.MaxReconnectInterval <= 0 || opts.MaxReconnectInterval < opts.MinReconnectInterval:
		return nil, fmt.Errorf("invalid max reconnect period: %s", opts.MaxReconnectInterval)
	}

	pgl := &PostgresListener{
		logger:  logger.WithField("component", "postgres_listener"),
		opts:    opts,
		handler: handler,
		closed:  make(chan struct{}),
		reconnectTotal: promclient.NewCounterVec(
			promclient.CounterOpts{
				Name: "gitaly_praefect_notifications_reconnects_total",
				Help: "Counts amount of reconnects to listen for notification from PostgreSQL",
			},
			[]string{"state"},
		),
	}

	if err := pgl.connect(); err != nil {
		if err := pgl.Close(); err != nil {
			pgl.logger.WithError(err).Error("releasing listener resources after failed to listen on it")
		}
		return nil, fmt.Errorf("connect: %w", err)
	}

	return pgl, nil
}

func (pgl *PostgresListener) connect() error {
	firstConnectionAttempt := true
	connectErrChan := make(chan error, 1)

	connectionLifecycle := func(eventType pq.ListenerEventType, err error) {
		pgl.reconnectTotal.WithLabelValues(listenerEventTypeToString(eventType)).Inc()

		switch eventType {
		case pq.ListenerEventConnectionAttemptFailed:
			pgl.logger.WithError(err).Error(listenerEventTypeToString(eventType))
			if firstConnectionAttempt {
				firstConnectionAttempt = false
				// if a first attempt to establish a connection to a remote is failed
				// we should not proceed as it won't be possible to distinguish between
				// temporary errors and initialization errors like invalid
				// connection address.
				connectErrChan <- err
			}
		case pq.ListenerEventConnected:
			// once the connection is established we can be sure that the connection
			// address is correct and all other errors could be considered as
			// a temporary, so listener will try to re-connect and proceed.
			pgl.async(pgl.ping)
			pgl.async(pgl.handleNotifications)

			close(connectErrChan) // to signal the connection was established without troubles
			firstConnectionAttempt = false

			pgl.handler.Connected()
		case pq.ListenerEventReconnected:
			pgl.handler.Connected()
		case pq.ListenerEventDisconnected:
			pgl.logger.WithError(err).Error(listenerEventTypeToString(eventType))
			pgl.handler.Disconnect(err)
		}
	}

	pgl.listener = pq.NewListener(pgl.opts.Addr, pgl.opts.MinReconnectInterval, pgl.opts.MaxReconnectInterval, connectionLifecycle)

	listenErrChan := make(chan error, 1)
	pgl.async(func() {
		// we need to start channel listeners in a parallel, otherwise if a bad connection string provided
		// the connectionLifecycle callback will always receive pq.ListenerEventConnectionAttemptFailed event.
		// When a listener is added it will produce an error on attempt to use a connection and re-connection
		// loop will be interrupted.
		listenErrChan <- pgl.listen()
	})

	if err := <-connectErrChan; err != nil {
		return err
	}

	return <-listenErrChan
}

func (pgl *PostgresListener) Close() error {
	defer func() {
		close(pgl.closed)
		pgl.wg.Wait()
	}()
	return pgl.listener.Close()
}

func listenerEventTypeToString(et pq.ListenerEventType) string {
	switch et {
	case pq.ListenerEventConnected:
		return "connected"
	case pq.ListenerEventDisconnected:
		return "disconnected"
	case pq.ListenerEventReconnected:
		return "reconnected"
	case pq.ListenerEventConnectionAttemptFailed:
		return "connection_attempt_failed"
	}
	return fmt.Sprintf("unknown: %d", et)
}

func (pgl *PostgresListener) listen() error {
	for _, channel := range pgl.opts.Channels {
		if err := pgl.listener.Listen(channel); err != nil {
			return err
		}
	}
	return nil
}

func (pgl *PostgresListener) handleNotifications() {
	for {
		select {
		case <-pgl.closed:
			return
		case notification, ok := <-pgl.listener.Notify:
			if !ok {
				// this happens when the Close is called on the listener
				return
			}

			if notification == nil {
				// this happens when pq.ListenerEventReconnected is emitted after a database
				// connection has been re-established after connection loss
				continue
			}

			pgl.handler.Notification(glsql.Notification{
				Channel: notification.Channel,
				Payload: notification.Extra,
			})
		}
	}
}

func (pgl *PostgresListener) ping() {
	for {
		select {
		case <-pgl.closed:
			return
		case <-time.After(pgl.opts.PingPeriod):
			if err := pgl.listener.Ping(); err != nil {
				pgl.logger.WithError(err).Error("health check ping failed")
			}
		}
	}
}

func (pgl *PostgresListener) async(f func()) {
	pgl.wg.Add(1)
	go func() {
		defer pgl.wg.Done()
		f()
	}()
}

func (pgl *PostgresListener) Describe(descs chan<- *promclient.Desc) {
	promclient.DescribeByCollect(pgl, descs)
}

func (pgl *PostgresListener) Collect(metrics chan<- promclient.Metric) {
	pgl.reconnectTotal.Collect(metrics)
}
