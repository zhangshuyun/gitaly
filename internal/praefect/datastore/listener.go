package datastore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
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
