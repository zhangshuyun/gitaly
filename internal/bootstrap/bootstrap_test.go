package bootstrap

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestBootstrap_unixListener(t *testing.T) {
	for _, tc := range []struct {
		desc               string
		hasParent          bool
		preexistingSocket  bool
		expectSocketExists bool
	}{
		{
			desc:               "no parent, no preexisting socket",
			hasParent:          false,
			preexistingSocket:  false,
			expectSocketExists: false,
		},
		{
			desc:              "no parent, preexisting socket",
			hasParent:         false,
			preexistingSocket: true,
			// On first boot, the bootstrapper is expected to remove any preexisting
			// sockets.
			expectSocketExists: false,
		},
		{
			desc:               "parent, no preexisting socket",
			hasParent:          true,
			preexistingSocket:  false,
			expectSocketExists: false,
		},
		{
			desc:              "parent, preexisting socket",
			hasParent:         true,
			preexistingSocket: true,
			// When we do have a parent, then we cannot remove the socket or otherwise
			// we might impact the parent process's ability to serve requests.
			expectSocketExists: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tempDir := testhelper.TempDir(t)
			socketPath := filepath.Join(tempDir, "gitaly-test-unix-socket")

			sentinel := &mockListener{}
			listen := func(network, addr string) (net.Listener, error) {
				require.Equal(t, "unix", network)
				require.Equal(t, socketPath, addr)
				if tc.expectSocketExists {
					require.FileExists(t, socketPath)
				} else {
					require.NoFileExists(t, socketPath)
				}

				return sentinel, nil
			}

			upgrader := &mockUpgrader{
				hasParent: tc.hasParent,
			}

			b, err := _new(upgrader, listen, false)
			require.NoError(t, err)

			if tc.preexistingSocket {
				require.NoError(t, os.WriteFile(socketPath, nil, 0o755))
			}

			listener, err := b.listen("unix", socketPath)
			require.NoError(t, err)
			require.Equal(t, sentinel, listener)
		})
	}
}

func TestBootstrap_listenerError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	b, upgrader, listeners := setup(t, ctx)

	waitCh := make(chan error)
	go func() { waitCh <- b.Wait(time.Hour, nil) }()

	// Signal readiness, but don't start the upgrade. Like this, we can close the listener in a
	// raceless manner and wait for the error to propagate.
	upgrader.readyCh <- nil

	// Inject a listener error.
	listeners["tcp"].errorCh <- assert.AnError

	require.Equal(t, assert.AnError, <-waitCh)
}

func TestBootstrap_signal(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			b, upgrader, _ := setup(t, ctx)

			waitCh := make(chan error)
			go func() { waitCh <- b.Wait(time.Hour, nil) }()

			// Start the upgrade, but don't unblock `Exit()` such that we'll be blocked
			// waiting on the parent.
			upgrader.readyCh <- nil

			// We can now kill ourselves. This signal should be retrieved by `Wait()`,
			// which would then return an error.
			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(sig))

			require.Equal(t, fmt.Errorf("received signal %q", sig), <-waitCh)
		})
	}
}

func TestBootstrap_gracefulTerminationStuck(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	b, upgrader, _ := setup(t, ctx)

	doneCh := make(chan struct{})
	err := performUpgrade(t, b, upgrader, time.Second, nil, func() {
		defer close(doneCh)

		// We block on context cancellation here, which essentially means that this won't
		// terminate and thus the graceful termination will be stuck.
		<-ctx.Done()
	})
	require.Equal(t, fmt.Errorf("graceful upgrade: grace period expired"), err)

	cancel()
	<-doneCh
}

func TestBootstrap_gracefulTerminationWithSignals(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			b, upgrader, _ := setup(t, ctx)

			doneCh := make(chan struct{})
			err := performUpgrade(t, b, upgrader, time.Hour, func() {
				self, err := os.FindProcess(os.Getpid())
				require.NoError(t, err)
				require.NoError(t, self.Signal(sig))
			}, func() {
				defer close(doneCh)
				// Block the upgrade indefinitely such that we can be sure that the
				// signal was processed.
				<-ctx.Done()
			})
			require.Equal(t, fmt.Errorf("graceful upgrade: force shutdown"), err)

			cancel()
			<-doneCh
		})
	}
}

func TestBootstrap_gracefulTerminationTimeoutWithListenerError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	b, upgrader, listeners := setup(t, ctx)

	doneCh := make(chan struct{})
	err := performUpgrade(t, b, upgrader, time.Second, nil, func() {
		defer close(doneCh)

		// We inject an error into the Unix socket to assert that this won't kill the server
		// immediately, but waits for the TCP connection to terminate as expected.
		listeners["unix"].errorCh <- assert.AnError

		// We block on context cancellation here, which essentially means that this won't
		// terminate.
		<-ctx.Done()
	})
	require.Equal(t, fmt.Errorf("graceful upgrade: grace period expired"), err)

	cancel()
	<-doneCh
}

func TestBootstrap_gracefulTermination(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	b, upgrader, _ := setup(t, ctx)

	require.Equal(t, fmt.Errorf("graceful upgrade: completed"), performUpgrade(t, b, upgrader, time.Hour, nil, nil))
}

func TestBootstrap_portReuse(t *testing.T) {
	b, err := New()
	require.NoError(t, err)

	l, err := b.listen("tcp", "localhost:")
	require.NoError(t, err, "failed to bind")

	addr := l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	l, err = b.listen("tcp", "localhost:"+port)
	require.NoError(t, err, "failed to bind")
	require.NoError(t, l.Close())

	b.upgrader.Stop()
}

func performUpgrade(
	t *testing.T,
	b *Bootstrap,
	upgrader *mockUpgrader,
	gracefulWait time.Duration,
	duringGracePeriodCallback func(),
	stopAction func(),
) error {
	waitCh := make(chan error)
	go func() { waitCh <- b.Wait(gracefulWait, stopAction) }()

	// Simulate an upgrade request after entering into the blocking b.Wait() and during the
	// slowRequest execution
	upgrader.readyCh <- nil
	upgrader.exitCh <- struct{}{}

	// We know that `exitCh` has been consumed, so we're now in the grace period where we wait
	// for the old server to exit.
	if duringGracePeriodCallback != nil {
		duringGracePeriodCallback()
	}

	return <-waitCh
}

func setup(t *testing.T, ctx context.Context) (*Bootstrap, *mockUpgrader, mockListeners) {
	u := &mockUpgrader{
		exitCh:  make(chan struct{}),
		readyCh: make(chan error),
	}

	b, err := _new(u, net.Listen, false)
	require.NoError(t, err)

	listeners := mockListeners{}
	start := func(network, address string) Starter {
		listeners[network] = &mockListener{}

		return func(listen ListenFunc, errors chan<- error) error {
			listeners[network].errorCh = errors
			listeners[network].listening = true
			return nil
		}
	}

	for network, address := range map[string]string{
		"tcp":  "127.0.0.1:0",
		"unix": "some-socket",
	} {
		b.RegisterStarter(start(network, address))
	}

	require.NoError(t, b.Start())
	require.Equal(t, 2, len(listeners))

	for _, listener := range listeners {
		require.True(t, listener.listening)
	}

	return b, u, listeners
}

type mockUpgrader struct {
	exitCh    chan struct{}
	readyCh   chan error
	hasParent bool
}

func (m *mockUpgrader) Exit() <-chan struct{} {
	return m.exitCh
}

func (m *mockUpgrader) Stop() {}

func (m *mockUpgrader) HasParent() bool {
	return m.hasParent
}

func (m *mockUpgrader) Ready() error {
	return <-m.readyCh
}

func (m *mockUpgrader) Upgrade() error {
	// To upgrade, we send a message on the exit channel. Like this, we can assert that the exit
	// signal has been consumed given that we'd otherwise block forever.
	m.exitCh <- struct{}{}
	return nil
}

type mockListener struct {
	net.Listener
	errorCh   chan<- error
	closed    bool
	listening bool
}

func (m *mockListener) Close() error {
	m.closed = true
	return nil
}

type mockListeners map[string]*mockListener
