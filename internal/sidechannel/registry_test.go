package sidechannel

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	const N = 10
	registry := NewRegistry()

	t.Run("waiter removed from the registry right after connection received", func(t *testing.T) {
		triggerCallback := make(chan struct{})
		waiter := registry.Register(func(conn *ClientConn) error {
			<-triggerCallback
			return nil
		})
		defer waiter.Close()

		require.Equal(t, 1, registry.waiting())

		client, _ := socketPair(t)
		require.NoError(t, registry.receive(waiter.id, client))
		require.Equal(t, 0, registry.waiting())

		close(triggerCallback)

		require.NoError(t, waiter.Wait())
		requireConnClosed(t, client)
	})

	t.Run("pull connections successfully", func(t *testing.T) {
		wg := sync.WaitGroup{}
		var servers []*ServerConn

		for i := 0; i < N; i++ {
			client, server := socketPair(t)
			servers = append(servers, newServerConn(server))

			wg.Add(1)
			go func(i int) {
				waiter := registry.Register(func(conn *ClientConn) error {
					if _, err := fmt.Fprintf(conn, "%d", i); err != nil {
						return err
					}

					return conn.CloseWrite()
				})
				defer waiter.Close()

				require.NoError(t, registry.receive(waiter.id, client))
				require.NoError(t, waiter.Wait())
				requireConnClosed(t, client)

				wg.Done()
			}(i)
		}

		for i := 0; i < N; i++ {
			out, err := io.ReadAll(servers[i])
			require.NoError(t, err)
			require.Equal(t, strconv.Itoa(i), string(out))
		}

		wg.Wait()
		require.Equal(t, 0, registry.waiting())
	})

	t.Run("push connection to non-existing ID", func(t *testing.T) {
		client, _ := socketPair(t)
		err := registry.receive(registry.nextID+1, client)
		require.EqualError(t, err, "sidechannel registry: ID not registered")
		requireConnClosed(t, client)
	})

	t.Run("pre-maturely close the waiter", func(t *testing.T) {
		waiter := registry.Register(func(conn *ClientConn) error { panic("never execute") })
		require.NoError(t, waiter.Close())
		require.Equal(t, 0, registry.waiting())
	})
}

func requireConnClosed(t *testing.T, conn net.Conn) {
	one := make([]byte, 1)
	_, err := conn.Read(one)
	require.Errorf(t, err, "use of closed network connection")
	_, err = conn.Write(one)
	require.Errorf(t, err, "use of closed network connection")
}

func socketPair(t *testing.T) (net.Conn, net.Conn) {
	conns := make([]net.Conn, 2)
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	for i, fd := range fds[:] {
		f := os.NewFile(uintptr(fd), "socket pair")
		c, err := net.FileConn(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		t.Cleanup(func() { c.Close() })
		conns[i] = c
	}
	return conns[0], conns[1]
}
