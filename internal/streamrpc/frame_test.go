package streamrpc

import (
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSendFrame(t *testing.T) {
	largeString := strings.Repeat("x", 0xfffff)

	testCases := []struct {
		desc string
		in   string
		out  string
		err  error
	}{
		{desc: "empty", out: "\x00\x00\x00\x00"},
		{desc: "not empty", in: "hello", out: "\x00\x00\x00\x05hello"},
		{desc: "very large", in: largeString, out: "\x00\x0f\xff\xff" + largeString},
		{desc: "too large", in: "z" + largeString, err: errFrameTooLarge},
	}

	type result struct {
		data string
		err  error
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client, server := socketPair(t)
			ch := make(chan result, 1)
			go func() {
				out, err := ioutil.ReadAll(server)
				ch <- result{data: string(out), err: err}
			}()

			err := sendFrame(client, []byte(tc.in), time.Now().Add(10*time.Second))
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			require.NoError(t, client.Close())

			res := <-ch
			require.NoError(t, res.err)
			require.Equal(t, tc.out, res.data)
		})
	}
}

func TestSendFrame_timeout(t *testing.T) {
	client, _ := socketPair(t)

	// Ensure frame is bigger than write buffer, so that sendFrame will
	// block. Otherwise we cannot observe the timeout behavior.
	frame := make([]byte, 10*1024)
	require.NoError(t, client.(*net.UnixConn).SetWriteBuffer(1024))

	err := sendFrame(client, frame, time.Now())
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestRecvFrame(t *testing.T) {
	largeString := strings.Repeat("x", 0xfffff)

	testCases := []struct {
		desc string
		out  string
		in   string
		err  error
	}{
		{desc: "empty", in: "\x00\x00\x00\x00", out: ""},
		{desc: "not empty", in: "\x00\x00\x00\x05hello", out: "hello"},
		{desc: "very large", in: "\x00\x0f\xff\xff" + largeString, out: largeString},
		{desc: "too large", in: "\x00\x10\x00\x00" + "z" + largeString, err: errFrameTooLarge},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client, server := socketPair(t)
			ch := make(chan error, 1)
			go func() {
				ch <- func() error {
					n, err := server.Write([]byte(tc.in))
					if n != len(tc.in) {
						return io.ErrShortWrite
					}
					return err
				}()
			}()

			out, err := recvFrame(client, time.Now().Add(10*time.Second))
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.out, string(out))

			require.NoError(t, <-ch)
		})
	}
}

func TestRecvFrame_timeout(t *testing.T) {
	client, _ := socketPair(t)
	_, err := recvFrame(client, time.Now())
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
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
