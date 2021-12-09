package datastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// Listener is designed to listen for PostgreSQL database NOTIFY events.
// It connects to the database with Listen method and starts to listen for
// events.
type Listener struct {
	pgxCfg *pgx.ConnConfig
}

// NewListener returns a listener that is ready to listen for PostgreSQL notifications.
func NewListener(conf config.DB) (*Listener, error) {
	pgxCfg, err := pgx.ParseConfig(glsql.DSN(conf, true))
	if err != nil {
		return nil, fmt.Errorf("connection config preparation: %w", err)
	}

	return &Listener{pgxCfg: pgxCfg}, nil
}

// Listen starts listening for the events. Each event is passed to the handler for processing.
// Listen is a blocking call, it returns in case context is cancelled or an error occurs while
// receiving notifications from the database.
func (l *Listener) Listen(ctx context.Context, handler glsql.ListenHandler, channels ...string) error {
	conn, err := pgx.ConnectConfig(ctx, l.pgxCfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	defer func() {
		// To exclude hang of the service on termination we wait for reasonable timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx) // we don't care much because we can't do anything
	}()

	query := compileListenQuery(channels)
	if _, err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("listen on channel(s): %w", err)
	}

	handler.Connected()

	for {
		nf, err := conn.WaitForNotification(ctx)
		if err != nil {
			handler.Disconnect(err)
			return fmt.Errorf("wait for notification: %w", err)
		}

		handler.Notification(glsql.Notification{
			Channel: nf.Channel,
			Payload: nf.Payload,
		})
	}
}

func compileListenQuery(channels []string) string {
	channelNames := make([]interface{}, len(channels))
	for i, channel := range channels {
		channelNames[i] = channel
	}
	statement := fmt.Sprintf(strings.Repeat("listen %s;", len(channels)), channelNames...)
	return statement
}

type metricsHandlerMiddleware struct {
	glsql.ListenHandler
	counter *promclient.CounterVec
}

func (mh metricsHandlerMiddleware) Connected() {
	mh.counter.WithLabelValues("connected").Inc()
	mh.ListenHandler.Connected()
}

func (mh metricsHandlerMiddleware) Disconnect(err error) {
	mh.counter.WithLabelValues("disconnected").Inc()
	mh.ListenHandler.Disconnect(err)
}

// ResilientListener allows listen for notifications resiliently.
type ResilientListener struct {
	conf           config.DB
	ticker         helper.Ticker
	logger         logrus.FieldLogger
	reconnectTotal *promclient.CounterVec
}

// NewResilientListener returns instance of the *ResilientListener.
func NewResilientListener(conf config.DB, ticker helper.Ticker, logger logrus.FieldLogger) *ResilientListener {
	return &ResilientListener{
		conf:   conf,
		ticker: ticker,
		logger: logger.WithField("component", "resilient_listener"),
		reconnectTotal: promclient.NewCounterVec(
			promclient.CounterOpts{
				Name: "gitaly_praefect_notifications_reconnects_total",
				Help: "Counts amount of reconnects to listen for notification from PostgreSQL",
			},
			[]string{"state"},
		),
	}
}

// Listen starts a new Listener and listens for the notifications on the channels.
// If error occurs and connection is closed/terminated another Listener is created
// after some await period. The method returns only when provided context is cancelled
// or invalid configuration is used.
func (rl *ResilientListener) Listen(ctx context.Context, handler glsql.ListenHandler, channels ...string) error {
	defer rl.ticker.Stop()
	for {
		lis, err := NewListener(rl.conf)
		if err != nil {
			return err
		}

		handler := metricsHandlerMiddleware{ListenHandler: handler, counter: rl.reconnectTotal}
		if err := lis.Listen(ctx, handler, channels...); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			rl.logger.WithError(err).
				WithField("channels", channels).
				Error("listening was interrupted")
		}

		rl.ticker.Reset()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rl.ticker.C():
		}
	}
}

// Describe return description of the metric.
func (rl *ResilientListener) Describe(descs chan<- *promclient.Desc) {
	promclient.DescribeByCollect(rl, descs)
}

// Collect returns set of metrics collected during execution.
func (rl *ResilientListener) Collect(metrics chan<- promclient.Metric) {
	rl.reconnectTotal.Collect(metrics)
}
