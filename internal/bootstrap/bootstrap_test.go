package bootstrap

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

type mockUpgrader struct {
	exit      chan struct{}
	hasParent bool
}

func (m *mockUpgrader) Exit() <-chan struct{} {
	return m.exit
}

func (m *mockUpgrader) HasParent() bool {
	return m.hasParent
}

func (m *mockUpgrader) Ready() error { return nil }

func (m *mockUpgrader) Upgrade() error {
	// to upgrade we close the exit channel
	close(m.exit)
	return nil
}

type testServer struct {
	server    *http.Server
	listeners map[string]net.Listener
	url       string
}

func (s *testServer) slowRequest(duration time.Duration) <-chan error {
	done := make(chan error)

	go func() {
		r, err := http.Get(fmt.Sprintf("%sslow?seconds=%d", s.url, int(duration.Seconds())))
		if r != nil {
			r.Body.Close()
		}

		done <- err
	}()

	return done
}

func TestCreateUnixListener(t *testing.T) {
	tempDir := testhelper.TempDir(t)

	socketPath := filepath.Join(tempDir, "gitaly-test-unix-socket")
	if err := os.Remove(socketPath); err != nil {
		require.True(t, os.IsNotExist(err), "cannot delete dangling socket: %v", err)
	}

	// simulate a dangling socket
	require.NoError(t, ioutil.WriteFile(socketPath, nil, 0755))

	listen := func(network, addr string) (net.Listener, error) {
		require.Equal(t, "unix", network)
		require.Equal(t, socketPath, addr)

		return net.Listen(network, addr)
	}
	u := &mockUpgrader{}
	b, err := _new(u, listen, false)
	require.NoError(t, err)

	// first boot
	l, err := b.listen("unix", socketPath)
	require.NoError(t, err, "failed to bind on first boot")
	require.NoError(t, l.Close())

	// simulate binding during an upgrade
	u.hasParent = true
	l, err = b.listen("unix", socketPath)
	require.NoError(t, err, "failed to bind on upgrade")
	require.NoError(t, l.Close())
}

func waitWithTimeout(t *testing.T, waitCh <-chan error, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		t.Fatal("time out waiting for waitCh")
	case waitErr := <-waitCh:
		return waitErr
	}

	return nil
}

func TestImmediateTerminationOnSocketError(t *testing.T) {
	b, server := makeBootstrap(t)

	waitCh := make(chan error)
	go func() { waitCh <- b.Wait(2 * time.Second) }()

	require.NoError(t, server.listeners["tcp"].Close(), "Closing first listener")

	err := waitWithTimeout(t, waitCh, 1*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "use of closed network connection")
}

func TestImmediateTerminationOnSignal(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			b, server := makeBootstrap(t)

			done := server.slowRequest(3 * time.Minute)

			waitCh := make(chan error)
			go func() { waitCh <- b.Wait(2 * time.Second) }()

			// make sure we are inside b.Wait() or we'll kill the test suite
			time.Sleep(100 * time.Millisecond)

			self, err := os.FindProcess(os.Getpid())
			require.NoError(t, err)
			require.NoError(t, self.Signal(sig))

			waitErr := waitWithTimeout(t, waitCh, 1*time.Second)
			require.Error(t, waitErr)
			require.Contains(t, waitErr.Error(), "received signal")
			require.Contains(t, waitErr.Error(), sig.String())

			server.server.Close()

			require.Error(t, <-done)
		})
	}
}

func TestGracefulTerminationStuck(t *testing.T) {
	b, server := makeBootstrap(t)

	err := testGracefulUpdate(t, server, b, 3*time.Second, 2*time.Second, nil)
	require.Contains(t, err.Error(), "grace period expired")
}

func TestGracefulTerminationWithSignals(t *testing.T) {
	self, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)

	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			b, server := makeBootstrap(t)

			err := testGracefulUpdate(t, server, b, 1*time.Second, 2*time.Second, func() {
				require.NoError(t, self.Signal(sig))
			})
			require.Contains(t, err.Error(), "force shutdown")
		})
	}
}

func TestGracefulTerminationServerErrors(t *testing.T) {
	b, server := makeBootstrap(t)

	done := make(chan error, 1)
	// This is a simulation of receiving a listener error during waitGracePeriod
	b.StopAction = func() {
		// we close the unix listener in order to test that the shutdown will not fail, but it keep waiting for the TCP request
		require.NoError(t, server.listeners["unix"].Close())

		// we start a new TCP request that if faster than the grace period
		req := server.slowRequest(time.Second)
		done <- <-req
		close(done)

		require.NoError(t, server.server.Shutdown(context.Background()))
	}

	err := testGracefulUpdate(t, server, b, 3*time.Second, 2*time.Second, nil)
	require.Contains(t, err.Error(), "grace period expired")

	require.NoError(t, <-done)
}

func TestGracefulTermination(t *testing.T) {
	b, server := makeBootstrap(t)

	// Using server.Close we bypass the graceful shutdown faking a completed shutdown
	b.StopAction = func() { server.server.Close() }

	err := testGracefulUpdate(t, server, b, 1*time.Second, 2*time.Second, nil)
	require.Contains(t, err.Error(), "completed")
}

func TestPortReuse(t *testing.T) {
	var addr string

	b, err := New()
	require.NoError(t, err)

	l, err := b.listen("tcp", "localhost:")
	require.NoError(t, err, "failed to bind")

	addr = l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	l, err = b.listen("tcp", "localhost:"+port)
	require.NoError(t, err, "failed to bind")
	require.NoError(t, l.Close())
}

func testGracefulUpdate(t *testing.T, server *testServer, b *Bootstrap, waitTimeout, gracefulWait time.Duration, duringGracePeriodCallback func()) error {
	waitCh := make(chan error)
	go func() { waitCh <- b.Wait(gracefulWait) }()

	// Start a slow request to keep the old server from shutting down immediately.
	req := server.slowRequest(2 * gracefulWait)

	// make sure slow request is being handled
	time.Sleep(100 * time.Millisecond)

	// Simulate an upgrade request after entering into the blocking b.Wait() and during the slowRequest execution
	require.NoError(t, b.upgrader.Upgrade())

	if duringGracePeriodCallback != nil {
		// make sure we are on the grace period
		time.Sleep(100 * time.Millisecond)

		duringGracePeriodCallback()
	}

	waitErr := waitWithTimeout(t, waitCh, waitTimeout)
	require.Error(t, waitErr)
	require.Contains(t, waitErr.Error(), "graceful upgrade")

	server.server.Close()

	clientErr := waitWithTimeout(t, req, 1*time.Second)
	require.Error(t, clientErr, "slow request not terminated after the grace period")

	return waitErr
}

func makeBootstrap(t *testing.T) (*Bootstrap, *testServer) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		sec, err := strconv.Atoi(r.URL.Query().Get("seconds"))
		require.NoError(t, err)

		time.Sleep(time.Duration(sec) * time.Second)

		w.WriteHeader(200)
	})

	s := http.Server{Handler: mux}
	u := &mockUpgrader{exit: make(chan struct{})}

	b, err := _new(u, net.Listen, false)
	require.NoError(t, err)

	b.StopAction = func() { require.NoError(t, s.Shutdown(context.Background())) }

	listeners := make(map[string]net.Listener)
	start := func(network, address string) Starter {
		return func(listen ListenFunc, errors chan<- error) error {
			l, err := listen(network, address)
			if err != nil {
				return err
			}
			listeners[network] = l

			go func() {
				errors <- s.Serve(l)
			}()

			return nil
		}
	}

	tempDir := testhelper.TempDir(t)

	for network, address := range map[string]string{
		"tcp":  "127.0.0.1:0",
		"unix": filepath.Join(tempDir, "gitaly-test-unix-socket"),
	} {
		b.RegisterStarter(start(network, address))
	}

	require.NoError(t, b.Start())
	require.Equal(t, 2, len(listeners))

	// test connection
	testAllListeners(t, listeners)

	addr := listeners["tcp"].Addr()
	url := fmt.Sprintf("http://%s/", addr.String())

	return b, &testServer{
		server:    &s,
		listeners: listeners,
		url:       url,
	}
}

func testAllListeners(t *testing.T, listeners map[string]net.Listener) {
	for network, listener := range listeners {
		addr := listener.Addr().String()

		// overriding Client.Transport.Dial we can connect to TCP and UNIX sockets
		client := &http.Client{
			Transport: &http.Transport{
				Dial: func(_, _ string) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		}

		// we don't need a real address because we forced it on Dial
		r, err := client.Get("http://fakeHost/")
		require.NoError(t, err)
		r.Body.Close()
		require.Equal(t, 200, r.StatusCode)
	}
}
