package streamrpcs

import (
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	t.Run("pull connections successfully", func(t *testing.T) {
		wg := sync.WaitGroup{}
		var readers []net.Conn

		for i := 0; i < 10; i++ {
			reader, writer := socketPair(t)
			readers = append(readers, reader)

			waiter, err := registry.Register(time.Now().Add(100 * time.Millisecond))
			require.NoError(t, err)

			wg.Add(1)
			go func(id int) {
				conn, err := waiter.Wait()
				require.NoError(t, err)
				require.NotNil(t, conn)

				_, err = conn.Write([]byte(strconv.Itoa(id)))
				require.NoError(t, err)

				conn.Close()
				wg.Done()
			}(i)

			go func() {
				err := registry.Push(waiter.Token, writer)
				require.NoError(t, err)
			}()
		}

		wg.Wait()
		for i := 0; i < 10; i++ {
			out, err := ioutil.ReadAll(readers[i])
			require.NoError(t, err)
			require.Equal(t, string(out), strconv.Itoa(i))
		}

		require.Equal(t, registry.Waiting(), 0)
	})

	t.Run("timeout while pulling connections", func(t *testing.T) {
		waiter, err := registry.Register(time.Now().Add(1 * time.Millisecond))
		require.NoError(t, err)
		require.Equal(t, registry.Waiting(), 1)

		conn, err := waiter.Wait()
		require.Nil(t, conn)
		require.EqualError(t, err, "stream rpc registry: timeout exceeds")

		require.Equal(t, registry.Waiting(), 0)
	})

	t.Run("push without having a waiting caller", func(t *testing.T) {
		waiter, err := registry.Register(time.Now().Add(1 * time.Millisecond))
		require.NoError(t, err)

		_, writer := socketPair(t)
		err = registry.Push(waiter.Token, writer)
		require.NoError(t, err)
		require.Equal(t, registry.Waiting(), 0)

		conn, err := waiter.Wait()
		require.NoError(t, err)
		require.Equal(t, conn, writer)
	})

	t.Run("push connection to non-existing connection", func(t *testing.T) {
		waiter, err := registry.Register(time.Now().Add(1 * time.Millisecond))
		require.NoError(t, err)

		_, writer := socketPair(t)
		err = registry.Push("not exsting token", writer)
		require.EqualError(t, err, "stream rpc registry: connection not registered")
		require.Equal(t, registry.Waiting(), 1)

		err = registry.Push(waiter.Token, writer)
		require.NoError(t, err)
		require.Equal(t, registry.Waiting(), 0)
	})

	t.Run("pull connection twice", func(t *testing.T) {
		waiter, err := registry.Register(time.Now().Add(1 * time.Millisecond))
		require.NoError(t, err)

		_, writer := socketPair(t)

		err = registry.Push(waiter.Token, writer)
		require.NoError(t, err)

		conn, err := waiter.Wait()
		require.NotNil(t, conn)
		require.NoError(t, err)

		conn, err = waiter.Wait()
		require.Nil(t, conn) // Not blocking. Channel already closed
		require.NoError(t, err)

		require.Equal(t, registry.Waiting(), 0)
	})

	t.Run("stop registry", func(t *testing.T) {
		errors := make(chan error)

		for i := 0; i < 10; i++ {
			waiter, err := registry.Register(time.Now().Add(100 * time.Millisecond))
			require.NoError(t, err)

			go func() {
				_, err := waiter.Wait()
				errors <- err
			}()
		}
		require.Equal(t, registry.Waiting(), 10)

		registry.Stop()
		require.Equal(t, registry.Waiting(), 0)

		for i := 0; i < 10; i++ {
			require.EqualError(t, <-errors, "stream rpc registry: register already stopped")
		}

		_, err := registry.Register(time.Now().Add(100 * time.Millisecond))
		require.EqualError(t, err, "stream rpc registry: register already stopped")

		_, writer := socketPair(t)
		err = registry.Push("token", writer)
		require.EqualError(t, err, "stream rpc registry: register already stopped")
	})
}

func socketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)

	conns := make([]net.Conn, 2)
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
