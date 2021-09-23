package sidechannel

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientWrite(t *testing.T) {
	largeString := strings.Repeat("a", maxChunkSize)

	testCases := []struct {
		desc string
		in   string
		out  string
		err  error
	}{
		{desc: "empty", out: ""},
		{desc: "1-byte string", in: "h", out: "0005h"},
		{desc: "short string", in: "hello", out: "0009hello"},
		{desc: "short string 2", in: "hello this world", out: "0014hello this world"},
		{
			desc: "large string",
			in:   largeString,
			out:  "fff0" + largeString,
		},
		{
			desc: "very large string",
			in:   largeString + "b",
			out:  "fff0" + largeString + "0005b",
		},
	}

	type result struct {
		data string
		err  error
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client, server := net.Pipe()
			ch := make(chan result, 1)
			go func() {
				out, err := io.ReadAll(server)
				ch <- result{data: string(out), err: err}
			}()

			cc := newClientConn(client)
			written, err := cc.Write([]byte(tc.in))
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			require.NoError(t, cc.close())
			require.Equal(t, written, len(tc.in))

			res := <-ch
			require.NoError(t, res.err)
			require.Equal(t, tc.out, res.data)
		})
	}
}

func TestServerRead(t *testing.T) {
	largeString := strings.Repeat("a", maxChunkSize)

	testCases := []struct {
		desc string
		in   string
		out  string
		err  error
	}{
		{desc: "empty", in: "0000", out: ""},
		{desc: "empty 2", in: "00040000", out: ""},
		{desc: "1-byte string", in: "0005h0000", out: "h"},
		{desc: "short string", in: "0009hello0000", out: "hello"},
		{desc: "short string 2", in: "0014hello this world0000", out: "hello this world"},
		{desc: "invalid header 1", in: "0001", err: fmt.Errorf("sidechannel: invalid header 1")},
		{desc: "invalid header 2", in: "0002", err: fmt.Errorf("sidechannel: invalid header 2")},
		{desc: "invalid header 3", in: "0003", err: fmt.Errorf("sidechannel: invalid header 3")},
		{desc: "multiple short strings", in: "0009hello0008this0009world0000", out: "hellothisworld"},
		{
			desc: "large string",
			in:   "fff0" + largeString + "0000",
			out:  largeString,
		},
		{
			desc: "very large string",
			in:   "fff0" + largeString + "0005b0000",
			out:  largeString + "b",
		},
		{desc: "flush packet", in: "0009hello0000trashtrash", out: "hello"},
		{desc: "unexpected closed without trailing 0000", in: "0009hello", err: io.ErrUnexpectedEOF},
	}

	type result struct {
		data string
		err  error
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client, server := net.Pipe()
			ch := make(chan result, 1)

			go func() {
				cc := newServerConn(server)
				out, err := io.ReadAll(cc)
				ch <- result{data: string(out), err: err}
			}()

			written, err := client.Write([]byte(tc.in))
			require.Equal(t, written, len(tc.in))
			require.NoError(t, err)

			client.Close()

			res := <-ch
			if tc.err != nil {
				require.Equal(t, tc.err, res.err)
				return
			}

			require.NoError(t, res.err)
			require.Equal(t, tc.out, res.data)
		})
	}
}

func TestHalfClose(t *testing.T) {
	type result struct {
		data string
		err  error
	}

	client, server := net.Pipe()
	clientCc := newClientConn(client)
	serverCc := newServerConn(server)

	ch := make(chan result, 1)

	go func() {
		out, err := io.ReadAll(serverCc)
		ch <- result{data: string(out), err: err}
	}()

	written, err := clientCc.Write([]byte("Ping"))
	require.Equal(t, written, 4)
	require.NoError(t, err)

	require.NoError(t, clientCc.CloseWrite())

	res := <-ch
	require.NoError(t, res.err)
	require.Equal(t, "Ping", res.data)

	_, err = clientCc.Write([]byte("Should not be sent"))
	require.EqualError(t, err, "sidechannel: write into a half-closed connection")

	go func() {
		out, err := io.ReadAll(clientCc)
		ch <- result{data: string(out), err: err}
	}()

	_, err = serverCc.Write([]byte("Pong"))
	require.NoError(t, err)

	serverCc.Close()

	res = <-ch
	require.NoError(t, res.err)
	require.Equal(t, "Pong", res.data)
}
