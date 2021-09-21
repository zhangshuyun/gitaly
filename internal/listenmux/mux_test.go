package listenmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

type handshakeFunc func(net.Conn, credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error)

func (hf handshakeFunc) Handshake(c net.Conn, ai credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
	return hf(c, ai)
}

const testmux = "test mux   "

func (hf handshakeFunc) Magic() string { return testmux }

func serverWithHandshaker(t *testing.T, h Handshaker) string {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	tc := New(insecure.NewCredentials())
	if h != nil {
		tc.Register(h)
	}

	s := grpc.NewServer(
		grpc.Creds(tc),
	)
	t.Cleanup(s.Stop)

	healthgrpc.RegisterHealthServer(s, health.NewServer())

	go func() { assert.NoError(t, s.Serve(l)) }()

	return l.Addr().String()
}

func checkHealth(t *testing.T, cc *grpc.ClientConn) {
	t.Helper()
	_, err := healthgrpc.NewHealthClient(cc).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	require.NoError(t, err)
}

func TestMux_normalClientNoMux(t *testing.T) {
	addr := serverWithHandshaker(t, nil)

	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	defer cc.Close()

	checkHealth(t, cc)
}

func TestMux_normalClientMuxIgnored(t *testing.T) {
	addr := serverWithHandshaker(t,
		handshakeFunc(func(net.Conn, credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
			t.Error("never called")
			return nil, nil, nil
		}),
	)

	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	defer cc.Close()

	checkHealth(t, cc)
}

func TestMux_muxClientPassesThrough(t *testing.T) {
	handshakerCalled := false

	addr := serverWithHandshaker(t,
		handshakeFunc(func(c net.Conn, ai credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
			handshakerCalled = true
			return c, ai, nil
		}),
	)

	cc, err := grpc.Dial(
		"ignored",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}

			if _, err := io.WriteString(c, testmux); err != nil {
				return nil, err
			}

			return c, nil
		}),
	)
	require.NoError(t, err)
	defer cc.Close()

	checkHealth(t, cc)

	require.True(t, handshakerCalled)
}

func readN(t *testing.T, r io.Reader, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	return buf
}

func TestMux_handshakerStealsConnection(t *testing.T) {
	connCh := make(chan net.Conn, 1)
	addr := serverWithHandshaker(t,
		handshakeFunc(func(c net.Conn, _ credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
			connCh <- c
			return nil, nil, credentials.ErrConnDispatched
		}),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)

		serverConn := <-connCh
		defer serverConn.Close()

		// Give grpc-go a chance to close the connection, which it shouldn't
		time.Sleep(100 * time.Millisecond)

		ping := readN(t, serverConn, 4)
		require.Equal(t, "ping", string(ping))

		_, err := io.WriteString(serverConn, "pong")
		require.NoError(t, err)
	}()

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()

	_, err = io.WriteString(c, testmux+"ping")
	require.NoError(t, err)

	pong := readN(t, c, 4)
	require.Equal(t, "pong", string(pong))

	<-done
}

func TestMux_handshakerReturnsError(t *testing.T) {
	addr := serverWithHandshaker(t,
		handshakeFunc(func(_ net.Conn, _ credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
			return nil, nil, errors.New("something went wrong")
		}),
	)

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()

	_, err = io.WriteString(c, testmux)
	require.NoError(t, err)

	require.NoError(t, c.SetDeadline(time.Now().Add(1*time.Second)))

	buf := make([]byte, 1)
	_, err = io.ReadFull(c, buf)
	require.Equal(t, io.EOF, err, "EOF tells us that grpc-go closed the connection")
}

func TestMux_concurrency(t *testing.T) {
	const N = 100

	// We want to open a lot of network connections. Raise the limits for the
	// process as far as we're allowed.
	var limit syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit))
	limit.Cur = limit.Max
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit))

	streamServerErrors := make(chan error, N)

	addr := serverWithHandshaker(t,
		handshakeFunc(func(c net.Conn, _ credentials.AuthInfo) (net.Conn, credentials.AuthInfo, error) {
			go func() {
				streamServerErrors <- func() error {
					defer c.Close()
					if _, err := io.Copy(c, c); err != nil {
						return err
					}
					return c.Close()
				}()
			}()

			return nil, nil, credentials.ErrConnDispatched
		}),
	)

	start := make(chan struct{})

	streamClientErrors := make(chan error, N)
	grpcHealthErrors := make(chan error, N)

	for i := 0; i < N; i++ {
		go func() {
			<-start
			streamClientErrors <- func() error {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					return err
				}
				defer c.Close()

				if err := c.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
					return err
				}

				if _, err := io.WriteString(c, testmux); err != nil {
					return err
				}

				buf := make([]byte, 128)
				if _, err = rand.Read(buf); err != nil {
					return err
				}

				if n, err := c.Write(buf); err != nil || n < len(buf) {
					return fmt.Errorf("write error or short write: %w", err)
				}

				if err := c.(*net.TCPConn).CloseWrite(); err != nil {
					return err
				}

				out, err := io.ReadAll(c)
				if err != nil {
					return err
				}
				if !bytes.Equal(buf, out) {
					return fmt.Errorf("expected %x, got %x", buf, out)
				}

				return c.Close()
			}()
		}()

		go func() {
			<-start
			grpcHealthErrors <- func() error {
				cc, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					return err
				}
				defer cc.Close()

				client := healthgrpc.NewHealthClient(cc)
				_, err = client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
				return err
			}()
		}()
	}

	close(start)

	for i := 0; i < N; i++ {
		require.NoError(t, <-streamServerErrors)
		require.NoError(t, <-streamClientErrors)
		require.NoError(t, <-grpcHealthErrors)
	}
}
