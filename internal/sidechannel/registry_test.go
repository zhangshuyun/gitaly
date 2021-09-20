package sidechannel

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	const N = 10
	registry := NewRegistry()

	t.Run("waiter removed from the registry right after connection received", func(t *testing.T) {
		triggerCallback := make(chan struct{})
		waiter := registry.Register(func(conn net.Conn) error {
			<-triggerCallback
			return nil
		})
		defer waiter.Close()

		require.Equal(t, 1, registry.waiting())

		client, _ := net.Pipe()
		require.NoError(t, registry.receive(waiter.id, client))
		require.Equal(t, 0, registry.waiting())

		close(triggerCallback)

		require.NoError(t, waiter.Wait())
		requireConnClosed(t, client)
	})

	t.Run("pull connections successfully", func(t *testing.T) {
		wg := sync.WaitGroup{}
		var servers []net.Conn

		for i := 0; i < N; i++ {
			client, server := net.Pipe()
			servers = append(servers, server)

			wg.Add(1)
			go func(i int) {
				waiter := registry.Register(func(conn net.Conn) error {
					_, err := fmt.Fprintf(conn, "%d", i)
					return err
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
		client, _ := net.Pipe()
		err := registry.receive(registry.nextID+1, client)
		require.EqualError(t, err, "sidechannel registry: ID not registered")
		requireConnClosed(t, client)
	})

	t.Run("pre-maturely close the waiter", func(t *testing.T) {
		waiter := registry.Register(func(conn net.Conn) error { panic("never execute") })
		require.NoError(t, waiter.Close())
		require.Equal(t, 0, registry.waiting())
	})
}

func requireConnClosed(t *testing.T, conn net.Conn) {
	one := make([]byte, 1)
	_, err := conn.Read(one)
	require.EqualError(t, err, "io: read/write on closed pipe")
	_, err = conn.Write(one)
	require.EqualError(t, err, "io: read/write on closed pipe")
}
