// +build postgres

package datastore

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestNewPostgresListener(t *testing.T) {
	for title, tc := range map[string]struct {
		opts      PostgresListenerOpts
		handler   glsql.ListenHandler
		expErrMsg string
	}{
		"invalid option: address": {
			opts:      PostgresListenerOpts{Addr: ""},
			expErrMsg: "address is invalid",
		},
		"invalid option: channels": {
			opts:      PostgresListenerOpts{Addr: "stub", Channels: nil},
			expErrMsg: "no channels to listen",
		},
		"invalid option: ping period": {
			opts:      PostgresListenerOpts{Addr: "stub", Channels: []string{""}, PingPeriod: -1},
			expErrMsg: "invalid ping period",
		},
		"invalid option: min reconnect period": {
			opts:      PostgresListenerOpts{Addr: "stub", Channels: []string{""}, MinReconnectInterval: 0},
			expErrMsg: "invalid min reconnect period",
		},
		"invalid option: max reconnect period": {
			opts:      PostgresListenerOpts{Addr: "stub", Channels: []string{""}, MinReconnectInterval: time.Second, MaxReconnectInterval: time.Millisecond},
			expErrMsg: "invalid max reconnect period",
		},
	} {
		t.Run(title, func(t *testing.T) {
			pgl, err := NewPostgresListener(testhelper.NewTestLogger(t), tc.opts, nil)
			if tc.expErrMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expErrMsg)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pgl)
		})
	}
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

func TestPostgresListener_Listen(t *testing.T) {
	db := getDB(t)

	logger := testhelper.NewTestLogger(t)

	newOpts := func() PostgresListenerOpts {
		opts := DefaultPostgresListenerOpts
		opts.Addr = getDBConfig(t).ToPQString(true)
		opts.MinReconnectInterval = time.Nanosecond
		opts.MaxReconnectInterval = time.Minute
		return opts
	}

	newChannel := func(i int) func() string {
		return func() string {
			i++
			return fmt.Sprintf("channel_%d", i)
		}
	}(0)

	notifyListener := func(t *testing.T, channelName, payload string) {
		t.Helper()

		_, err := db.Exec(fmt.Sprintf(`NOTIFY %s, '%s'`, channelName, payload))
		assert.NoError(t, err)
	}

	listenNotify := func(t *testing.T, opts PostgresListenerOpts, numNotifiers int, payloads []string) (*PostgresListener, []string) {
		t.Helper()

		start := make(chan struct{})
		done := make(chan struct{})
		defer func() { <-done }()

		numResults := len(payloads) * numNotifiers
		allReceivedChan := make(chan struct{})

		go func() {
			defer close(done)

			<-start

			var wg sync.WaitGroup
			for i := 0; i < numNotifiers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, payload := range payloads {
						for _, channel := range opts.Channels {
							notifyListener(t, channel, payload)
						}
					}
				}()
			}
			wg.Wait()

			select {
			case <-time.After(time.Second):
				assert.FailNow(t, "notification propagation takes too long")
			case <-allReceivedChan:
			}
		}()

		result := make([]string, numResults)
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
		pgl, err := NewPostgresListener(logger, opts, handler)
		require.NoError(t, err)

		return pgl, result
	}

	disconnectListener := func(t *testing.T, channelName string) {
		t.Helper()

		q := `SELECT PG_TERMINATE_BACKEND(pid) FROM PG_STAT_ACTIVITY WHERE datname = $1 AND query = $2`
		res, err := db.Exec(q, databaseName, fmt.Sprintf("LISTEN %q", channelName))
		if assert.NoError(t, err) {
			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.EqualValues(t, 1, affected)
		}
	}

	waitFor := func(t *testing.T, c <-chan struct{}, d time.Duration) {
		t.Helper()

		select {
		case <-time.After(d):
			require.FailNow(t, "it takes too long")
		case <-c:
			// proceed
		}
	}

	t.Run("single handler and single notifier", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{newChannel()}

		payloads := []string{"this", "is", "a", "payload"}

		listener, result := listenNotify(t, opts, 1, payloads)
		defer func() { require.NoError(t, listener.Close()) }()
		require.Equal(t, payloads, result)
	})

	t.Run("single handler and multiple notifiers", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{newChannel()}

		numNotifiers := 10

		payloads := []string{"this", "is", "a", "payload"}
		var expResult []string
		for i := 0; i < numNotifiers; i++ {
			expResult = append(expResult, payloads...)
		}

		listener, result := listenNotify(t, opts, numNotifiers, payloads)
		defer func() { require.NoError(t, listener.Close()) }()
		require.ElementsMatch(t, expResult, result, "there must be no additional data, only expected")
	})

	t.Run("multiple channels", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{"channel_1", "channel_2"}

		start := make(chan struct{})
		resultChan := make(chan glsql.Notification)
		handler := mockListenHandler{
			OnNotification: func(n glsql.Notification) { resultChan <- n },
			OnConnected:    func() { close(start) },
		}

		listener, err := NewPostgresListener(logger, opts, handler)
		require.NoError(t, err)
		defer func() { require.NoError(t, listener.Close()) }()

		waitFor(t, start, time.Minute)

		var expectedNotifications []glsql.Notification
		for i := 0; i < 3; i++ {
			for _, channel := range opts.Channels {
				payload := fmt.Sprintf("%s:%d", channel, i)
				notifyListener(t, channel, payload)

				expectedNotifications = append(expectedNotifications, glsql.Notification{
					Channel: channel,
					Payload: payload,
				})
			}
		}

		tooLong := time.After(time.Minute)
		var actualNotifications []glsql.Notification
		for i := 0; i < len(expectedNotifications); i++ {
			select {
			case <-tooLong:
				require.FailNow(t, "no notifications for too long")
			case notification := <-resultChan:
				actualNotifications = append(actualNotifications, notification)
			}
		}

		require.Equal(t, expectedNotifications, actualNotifications)
	})

	t.Run("invalid connection", func(t *testing.T) {
		opts := newOpts()
		opts.Addr = "invalid-address"
		opts.Channels = []string{"stub"}

		logger, hook := test.NewNullLogger()

		_, err := NewPostgresListener(logger, opts, mockListenHandler{})
		require.Error(t, err)
		require.Regexp(t, "^connect: .*invalid-address.*", err.Error())

		entries := hook.AllEntries()
		require.GreaterOrEqualf(t, len(entries), 1, "it should log at least failed initial attempt to connect")
		require.Equal(t, "connection_attempt_failed", entries[0].Message)
	})

	t.Run("channel used more then once", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{"stub1", "stub2", "stub1"}

		_, err := NewPostgresListener(logger, opts, mockListenHandler{})
		require.True(t, errors.Is(err, pq.ErrChannelAlreadyOpen), err)
	})

	t.Run("connection interruption", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{newChannel()}

		connected := make(chan struct{}, 1)
		handler := mockListenHandler{OnConnected: func() { connected <- struct{}{} }}

		listener, err := NewPostgresListener(logger, opts, handler)
		require.NoError(t, err)

		waitFor(t, connected, time.Minute)
		disconnectListener(t, opts.Channels[0])
		waitFor(t, connected, time.Minute)
		require.NoError(t, listener.Close())

		err = testutil.CollectAndCompare(listener, strings.NewReader(`
			# HELP gitaly_praefect_notifications_reconnects_total Counts amount of reconnects to listen for notification from PostgreSQL
			# TYPE gitaly_praefect_notifications_reconnects_total counter
			gitaly_praefect_notifications_reconnects_total{state="connected"} 1
			gitaly_praefect_notifications_reconnects_total{state="disconnected"} 1
			gitaly_praefect_notifications_reconnects_total{state="reconnected"} 1
		`))
		require.NoError(t, err)
	})

	t.Run("persisted connection interruption", func(t *testing.T) {
		opts := newOpts()
		opts.Channels = []string{newChannel()}

		connected := make(chan struct{}, 1)
		disconnected := make(chan struct{}, 1)
		handler := mockListenHandler{
			OnConnected: func() { connected <- struct{}{} },
			OnDisconnect: func(err error) {
				assert.Error(t, err, "disconnect event should always receive non-nil error")
				disconnected <- struct{}{}
			},
		}

		listener, err := NewPostgresListener(logger, opts, handler)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			waitFor(t, connected, time.Minute)
			disconnectListener(t, opts.Channels[0])
			waitFor(t, disconnected, time.Minute)
		}

		// this additional step is required to have exactly 3 "reconnected" metric value, otherwise it could
		// be 2 or 3 - it depends if it was quick enough to re-establish a new connection or not.
		waitFor(t, connected, time.Minute)

		require.NoError(t, listener.Close())

		err = testutil.CollectAndCompare(listener, strings.NewReader(`
			# HELP gitaly_praefect_notifications_reconnects_total Counts amount of reconnects to listen for notification from PostgreSQL
			# TYPE gitaly_praefect_notifications_reconnects_total counter
			gitaly_praefect_notifications_reconnects_total{state="connected"} 1
			gitaly_praefect_notifications_reconnects_total{state="disconnected"} 3
			gitaly_praefect_notifications_reconnects_total{state="reconnected"} 3
		`))
		require.NoError(t, err)
	})
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

func TestPostgresListener_Listen_repositories_delete(t *testing.T) {
	db := getDB(t)

	const channel = "repositories_updates"

	testListener(
		t,
		"repositories_updates",
		func(t *testing.T) {
			_, err := db.DB.Exec(`
				INSERT INTO repositories
				VALUES ('praefect-1', '/path/to/repo/1', 1),
					('praefect-1', '/path/to/repo/2', 1),
					('praefect-1', '/path/to/repo/3', 0),
					('praefect-2', '/path/to/repo/1', 1)`)
			require.NoError(t, err)
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`DELETE FROM repositories WHERE generation > 0`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, channel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{
				{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo/1", "/path/to/repo/2"}},
				{VirtualStorage: "praefect-2", RelativePaths: []string{"/path/to/repo/1"}},
			})
		},
	)
}

func TestPostgresListener_Listen_storage_repositories_insert(t *testing.T) {
	db := getDB(t)

	const channel = "storage_repositories_updates"

	testListener(
		t,
		channel,
		func(t *testing.T) {},
		func(t *testing.T) {
			_, err := db.DB.Exec(`
				INSERT INTO storage_repositories
				VALUES ('praefect-1', '/path/to/repo', 'gitaly-1', 0),
					('praefect-1', '/path/to/repo', 'gitaly-2', 0)`,
			)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, channel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func TestPostgresListener_Listen_storage_repositories_update(t *testing.T) {
	db := getDB(t)

	const channel = "storage_repositories_updates"

	testListener(
		t,
		channel,
		func(t *testing.T) {
			_, err := db.DB.Exec(`INSERT INTO storage_repositories VALUES ('praefect-1', '/path/to/repo', 'gitaly-1', 0)`)
			require.NoError(t, err)
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`UPDATE storage_repositories SET generation = generation + 1`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, channel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func TestPostgresListener_Listen_storage_empty_notification(t *testing.T) {
	db := getDB(t)

	const channel = "storage_repositories_updates"

	testListener(
		t,
		channel,
		func(t *testing.T) {},
		func(t *testing.T) {
			_, err := db.DB.Exec(`UPDATE storage_repositories SET generation = 1`)
			require.NoError(t, err)
		},
		nil, // no notification events expected
	)
}

func TestPostgresListener_Listen_storage_repositories_delete(t *testing.T) {
	db := getDB(t)

	const channel = "storage_repositories_updates"

	testListener(
		t,
		channel,
		func(t *testing.T) {
			_, err := db.DB.Exec(`
				INSERT INTO storage_repositories (virtual_storage, relative_path, storage, generation)
				VALUES ('praefect-1', '/path/to/repo', 'gitaly-1', 0)`,
			)
			require.NoError(t, err)
		},
		func(t *testing.T) {
			_, err := db.DB.Exec(`DELETE FROM storage_repositories`)
			require.NoError(t, err)
		},
		func(t *testing.T, n glsql.Notification) {
			require.Equal(t, channel, n.Channel)
			requireEqualNotificationEntries(t, n.Payload, []notificationEntry{{VirtualStorage: "praefect-1", RelativePaths: []string{"/path/to/repo"}}})
		},
	)
}

func testListener(t *testing.T, channel string, setup func(t *testing.T), trigger func(t *testing.T), verifier func(t *testing.T, notification glsql.Notification)) {
	setup(t)

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
		}
	}

	opts := DefaultPostgresListenerOpts
	opts.Addr = getDBConfig(t).ToPQString(true)
	opts.Channels = []string{channel}

	handler := mockListenHandler{OnNotification: callback, OnConnected: func() { close(readyChan) }}

	pgl, err := NewPostgresListener(testhelper.NewTestLogger(t), opts, handler)
	require.NoError(t, err)
	defer pgl.Close()

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
}
