package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestNewListener(t *testing.T) {
	t.Parallel()

	t.Run("bad configuration", func(t *testing.T) {
		_, err := NewListener(config.DB{Host: "tcp://i-do-not-exist", SSLMode: "invalid"})
		require.Error(t, err)
		require.Regexp(t, "connection config preparation:.*`host=tcp://i-do-not-exist sslmode=invalid.*`", err.Error())
	})
}

func TestListener_Listen(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)

	lis, err := NewListener(testdb.GetConfig(t, db.Name))
	require.NoError(t, err)

	newChannel := func(i int) func() string {
		return func() string {
			i++
			return fmt.Sprintf("channel_%d", i)
		}
	}(0)

	notifyListener := func(t *testing.T, channels []string, payload string) {
		t.Helper()
		for _, channel := range channels {
			_, err := db.Exec(fmt.Sprintf(`NOTIFY %s, '%s'`, channel, payload))
			assert.NoError(t, err)
		}
	}

	listenNotify := func(t *testing.T, lis *Listener, channels []string, numNotifiers int, payloads []string) []string {
		t.Helper()

		start := make(chan struct{})
		go func() {
			<-start

			for i := 0; i < numNotifiers; i++ {
				go func() {
					for _, payload := range payloads {
						notifyListener(t, channels, payload)
					}
				}()
			}
		}()

		numResults := len(channels) * len(payloads) * numNotifiers
		result := make([]string, numResults)
		allReceivedChan := make(chan struct{})
		callback := func(idx int) func(n glsql.Notification) {
			return func(n glsql.Notification) {
				idx++
				result[idx] = n.Payload
				if idx+1 == numResults {
					close(allReceivedChan)
				}
			}
		}(-1)

		handler := mockListenHandler{OnNotification: callback, OnConnected: func() { close(start) }}
		ctx, cancel := context.WithCancel(testhelper.Context(t))
		allDone := make(chan struct{})
		go func() {
			waitFor(t, allReceivedChan)
			cancel()
			close(allDone)
		}()
		err := lis.Listen(ctx, handler, channels...)
		<-allDone
		assert.True(t, errors.Is(err, context.Canceled), err)
		return result
	}

	t.Run("listen on bad channel", func(t *testing.T) {
		ctx := testhelper.Context(t)
		err := lis.Listen(ctx, mockListenHandler{}, "bad channel")
		require.EqualError(t, err, `listen on channel(s): ERROR: syntax error at or near "channel" (SQLSTATE 42601)`)
	})

	t.Run("single listener and single notifier", func(t *testing.T) {
		channel := newChannel()
		payloads := []string{"this", "is", "a", "payload"}
		result := listenNotify(t, lis, []string{channel}, 1, payloads)
		require.Equal(t, payloads, result)
	})

	t.Run("single listener and multiple notifiers", func(t *testing.T) {
		channel := newChannel()

		const numNotifiers = 10

		payloads := []string{"this", "is", "a", "payload"}
		var expResult []string
		for i := 0; i < numNotifiers; i++ {
			expResult = append(expResult, payloads...)
		}

		result := listenNotify(t, lis, []string{channel}, numNotifiers, payloads)
		require.ElementsMatch(t, expResult, result, "there must be no additional data, only expected")
	})

	t.Run("listen multiple channels", func(t *testing.T) {
		channel1 := newChannel()
		channel2 := newChannel()

		result := listenNotify(t, lis, []string{channel1, channel2}, 1, []string{"payload"})
		require.Equal(t, []string{"payload", "payload"}, result)
	})

	t.Run("sequential Listen calls are allowed", func(t *testing.T) {
		channel1 := newChannel()
		result := listenNotify(t, lis, []string{channel1}, 1, []string{"payload-1"})
		require.Equal(t, []string{"payload-1"}, result)

		channel2 := newChannel()
		result2 := listenNotify(t, lis, []string{channel2}, 1, []string{"payload-2"})
		require.Equal(t, []string{"payload-2"}, result2)
	})

	t.Run("connection interruption", func(t *testing.T) {
		lis, err := NewListener(testdb.GetConfig(t, db.Name))
		require.NoError(t, err)

		channel := newChannel()

		connected := make(chan struct{})
		disconnected := make(chan struct{})
		handler := mockListenHandler{
			OnConnected:  func() { close(connected) },
			OnDisconnect: func(error) { close(disconnected) },
		}
		ctx := testhelper.Context(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			err := lis.Listen(ctx, handler, channel)
			var pgErr *pgconn.PgError
			if assert.True(t, errors.As(err, &pgErr)) {
				const adminShutdownCode = "57P01"
				assert.Equal(t, adminShutdownCode, pgErr.Code)
				assert.Equal(t, "FATAL", pgErr.Severity)
			}
		}()

		waitFor(t, connected)
		disconnectListener(t, db, channel)
		waitFor(t, disconnected)
		<-done
	})
}

func TestResilientListener_Listen(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	ctx, cancel := context.WithCancel(testhelper.Context(t))

	const channel = "channel_z"
	logger, hook := test.NewNullLogger()
	connected := make(chan struct{})
	disconnected := make(chan struct{})
	handler := mockListenHandler{
		OnConnected:  func() { connected <- struct{}{} },
		OnDisconnect: func(error) { disconnected <- struct{}{} },
	}

	lis := NewResilientListener(
		testdb.GetConfig(t, db.Name),
		helper.NewCountTicker(1, func() {}),
		logger,
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := lis.Listen(ctx, handler, channel)
		require.True(t, errors.Is(err, context.Canceled), err)
	}()

	waitFor(t, connected)
	disconnectListener(t, db, channel)
	waitFor(t, disconnected)
	waitFor(t, connected)
	cancel()
	waitFor(t, disconnected)
	<-done

	entries := hook.AllEntries()
	require.Len(t, entries, 1)
	require.Equal(t, "listening was interrupted", entries[0].Message)
	require.Equal(t, []string{channel}, entries[0].Data["channels"])

	require.NoError(t, testutil.CollectAndCompare(lis, strings.NewReader(`
		# HELP gitaly_praefect_notifications_reconnects_total Counts amount of reconnects to listen for notification from PostgreSQL
		# TYPE gitaly_praefect_notifications_reconnects_total counter
		gitaly_praefect_notifications_reconnects_total{state="connected"} 2
		gitaly_praefect_notifications_reconnects_total{state="disconnected"} 2
	`)))
}

func waitFor(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-time.After(30 * time.Second):
		require.FailNow(t, "it takes too long")
	case <-c:
		// proceed
	}
}

func disconnectListener(t *testing.T, db testdb.DB, channel string) {
	t.Helper()
	res, err := db.Exec(
		`SELECT PG_TERMINATE_BACKEND(pid) FROM PG_STAT_ACTIVITY WHERE datname = $1 AND query = $2`,
		db.Name,
		"listen "+channel+";",
	)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)
}

func TestListener_Listen_repositories_delete(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)
	ctx := testhelper.Context(t)

	verifyListener(
		t,
		ctx,
		dbConf,
		RepositoriesUpdatesChannel,
		func(t *testing.T) {
			_, err := db.DB.Exec(`
				INSERT INTO repositories
				VALUES ('praefect-1', '/path/to/repo/1', 1, 1),
					('praefect-1', '/path/to/repo/2', 1, 2),
					('praefect-1', '/path/to/repo/3', 0, 3),
					('praefect-2', '/path/to/repo/1', 1, 4)`)
			require.NoError(t, err)
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`DELETE FROM repositories WHERE generation > 0`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, RepositoriesUpdatesChannel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{
				{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo/1", "/path/to/repo/2"}},
				{VirtualStorage: "praefect-2", RelativePaths: []string{"/path/to/repo/1"}},
			})
		},
	)
}

func TestListener_Listen_storage_repositories_insert(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)
	ctx := testhelper.Context(t)

	verifyListener(
		t,
		ctx,
		dbConf,
		StorageRepositoriesUpdatesChannel,
		func(t *testing.T) {
			rs := NewPostgresRepositoryStore(db, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, "praefect-1", "/path/to/repo", "replica-path", "primary", nil, nil, true, false))
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`
				INSERT INTO storage_repositories
				VALUES ('praefect-1', '/path/to/repo', 'gitaly-1', 0, 1),
					('praefect-1', '/path/to/repo', 'gitaly-2', 0, 1)`,
			)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, StorageRepositoriesUpdatesChannel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func TestListener_Listen_storage_repositories_update(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)
	ctx := testhelper.Context(t)

	verifyListener(
		t,
		ctx,
		dbConf,
		StorageRepositoriesUpdatesChannel,
		func(t *testing.T) {
			rs := NewPostgresRepositoryStore(db, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, "praefect-1", "/path/to/repo", "replica-path", "gitaly-1", nil, nil, true, false))
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`UPDATE storage_repositories SET generation = generation + 1`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, StorageRepositoriesUpdatesChannel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func TestListener_Listen_storage_empty_notification(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)
	ctx := testhelper.Context(t)

	verifyListener(
		t,
		ctx,
		dbConf,
		StorageRepositoriesUpdatesChannel,
		func(t *testing.T) {},
		func(t *testing.T) {
			_, err := db.DB.Exec(`UPDATE storage_repositories SET generation = 1`)
			require.NoError(t, err)
		},
		nil, // no notification events expected
	)
}

func TestListener_Listen_storage_repositories_delete(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	dbConf := testdb.GetConfig(t, db.Name)
	ctx := testhelper.Context(t)

	verifyListener(
		t,
		ctx,
		dbConf,
		StorageRepositoriesUpdatesChannel,
		func(t *testing.T) {
			rs := NewPostgresRepositoryStore(db, nil)
			require.NoError(t, rs.CreateRepository(ctx, 1, "praefect-1", "/path/to/repo", "replica-path", "gitaly-1", nil, nil, true, false))
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`DELETE FROM storage_repositories`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, StorageRepositoriesUpdatesChannel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func verifyListener(t *testing.T, ctx context.Context, dbConf config.DB, channel string, setup func(t *testing.T), trigger func(t *testing.T), verifier func(t *testing.T, notification glsql.Notification)) {
	setup(t)

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	readyChan := make(chan struct{})
	receivedChan := make(chan struct{})
	var notification glsql.Notification

	callback := func(n glsql.Notification) {
		select {
		case <-receivedChan:
			return
		default:
			notification = n
			close(receivedChan)
			runCancel()
		}
	}

	handler := mockListenHandler{OnNotification: callback, OnConnected: func() { close(readyChan) }}

	lis, err := NewListener(dbConf)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := lis.Listen(runCtx, handler, channel)
		assert.True(t, errors.Is(err, context.Canceled), err)
	}()

	select {
	case <-time.After(time.Second):
		require.FailNow(t, "no connection for too long period")
	case <-readyChan:
	}

	trigger(t)

	select {
	case <-time.After(time.Second):
		if verifier == nil {
			// no notifications expected
			return
		}
		require.FailNow(t, "no notifications for too long period")
	case <-receivedChan:
	}

	if verifier == nil {
		require.Failf(t, "no notifications expected", "received: %v", notification)
	}
	verifier(t, notification)
	waitFor(t, done)
}

func requireEqualNotificationEntries(t *testing.T, d string, entries []notificationEntry) {
	t.Helper()

	var nes []notificationEntry
	require.NoError(t, json.NewDecoder(strings.NewReader(d)).Decode(&nes))

	for _, es := range [][]notificationEntry{entries, nes} {
		for _, e := range es {
			sort.Strings(e.RelativePaths)
		}
		sort.Slice(es, func(i, j int) bool { return es[i].VirtualStorage < es[j].VirtualStorage })
	}

	require.EqualValues(t, entries, nes)
}

type mockListenHandler struct {
	OnNotification func(glsql.Notification)
	OnDisconnect   func(error)
	OnConnected    func()
}

func (mlh mockListenHandler) Notification(n glsql.Notification) {
	if mlh.OnNotification != nil {
		mlh.OnNotification(n)
	}
}

func (mlh mockListenHandler) Disconnect(err error) {
	if mlh.OnDisconnect != nil {
		mlh.OnDisconnect(err)
	}
}

func (mlh mockListenHandler) Connected() {
	if mlh.OnConnected != nil {
		mlh.OnConnected()
	}
}
