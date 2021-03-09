package pktline

import (
	"bytes"
	"errors"
	"io"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	largestString = strings.Repeat("z", 65516)
)

func TestScanner(t *testing.T) {
	largestPacket := "fff0" + largestString
	testCases := []struct {
		desc string
		in   string
		out  []string
		fail bool
	}{
		{
			desc: "happy path",
			in:   "0010hello world!000000010010hello world!",
			out:  []string{"0010hello world!", "0000", "0001", "0010hello world!"},
		},
		{
			desc: "large input",
			in:   "0010hello world!0000" + largestPacket + "0000",
			out:  []string{"0010hello world!", "0000", largestPacket, "0000"},
		},
		{
			desc: "missing byte middle",
			in:   "0010hello world!00000010010hello world!",
			out:  []string{"0010hello world!", "0000", "0010010hello wor"},
			fail: true,
		},
		{
			desc: "unfinished prefix",
			in:   "0010hello world!000",
			out:  []string{"0010hello world!"},
			fail: true,
		},
		{
			desc: "short read in data, only prefix",
			in:   "0010hello world!0005",
			out:  []string{"0010hello world!"},
			fail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			scanner := NewScanner(strings.NewReader(tc.in))
			var output []string
			for scanner.Scan() {
				output = append(output, scanner.Text())
			}

			if tc.fail {
				require.Error(t, scanner.Err())
			} else {
				require.NoError(t, scanner.Err())
			}

			require.Equal(t, tc.out, output)
		})
	}
}

func TestData(t *testing.T) {
	testCases := []struct {
		in  string
		out string
	}{
		{in: "0008abcd", out: "abcd"},
		{in: "invalid packet", out: "lid packet"},
		{in: "0005wrong length prefix", out: "wrong length prefix"},
		{in: "0000", out: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.out, string(Data([]byte(tc.in))))
		})
	}
}

func TestIsFlush(t *testing.T) {
	testCases := []struct {
		in    string
		flush bool
	}{
		{in: "0008abcd", flush: false},
		{in: "invalid packet", flush: false},
		{in: "0000", flush: true},
		{in: "0001", flush: false},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			require.Equal(t, tc.flush, IsFlush([]byte(tc.in)))
		})
	}
}

func TestWriteString(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
		out  string
		fail bool
	}{
		{
			desc: "empty string",
			in:   "",
			out:  "0004",
		},
		{
			desc: "small string",
			in:   "hello world!",
			out:  "0010hello world!",
		},
		{
			desc: "largest possible string",
			in:   largestString,
			out:  "fff0" + largestString,
		},
		{
			desc: "string that is too large",
			in:   "x" + largestString,
			fail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			w := &bytes.Buffer{}
			n, err := WriteString(w, tc.in)

			if tc.fail {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tc.in), n, "number of bytes written reported by WriteString")

			require.Equal(t, tc.out, w.String())
		})
	}
}

func TestWriteFlush(t *testing.T) {
	w := &bytes.Buffer{}
	require.NoError(t, WriteFlush(w))
	require.Equal(t, "0000", w.String())
}

func TestSidebandWriter_boundaries(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
		band byte
		out  string
	}{
		{
			desc: "empty",
			in:   "",
			band: 0,
			out:  "",
		},
		{
			desc: "1 byte",
			in:   "x",
			band: 1,
			out:  "0006\x01x",
		},
		{
			desc: "65514 bytes",
			in:   strings.Repeat("x", 65514),
			band: 255,
			out:  "ffef\xff" + strings.Repeat("x", 65514),
		},
		{
			desc: "65515 bytes: max per sideband packets",
			in:   strings.Repeat("x", 65515),
			band: 254,
			out:  "fff0\xfe" + strings.Repeat("x", 65515),
		},
		{
			desc: "65516 bytes: split across two packets",
			in:   strings.Repeat("x", 65516),
			band: 253,
			out:  "fff0\xfd" + strings.Repeat("x", 65515) + "0006\xfdx",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := NewSidebandWriter(buf).Writer(tc.band)

			n, err := w.Write([]byte(tc.in))
			require.NoError(t, err)
			require.Equal(t, n, len(tc.in))

			require.Equal(t, tc.out, buf.String())
		})
	}
}

func TestSidebandWriter_concurrency(t *testing.T) {
	const N = math.MaxUint8 + 1

	buf := &bytes.Buffer{}
	sw := NewSidebandWriter(buf)
	inputs := make([][]byte, N)
	writeErrors := make(chan error, N)
	start := make(chan struct{})

	for i := 0; i < N; i++ {
		inputs[i] = make([]byte, 1024)
		_, _ = rand.Read(inputs[i]) // math/rand.Read never fails

		go func(i int) {
			<-start
			w := sw.Writer(byte(i))
			writeErrors <- func() error {
				data := inputs[i]
				for j := 0; j < len(data); j++ {
					n, err := w.Write(data[j : j+1])
					if err != nil {
						return err
					}
					if n != 1 {
						return io.ErrShortWrite
					}
				}

				return nil
			}()
		}(i)
	}

	close(start)
	for i := 0; i < N; i++ {
		require.NoError(t, <-writeErrors)
	}

	outputs := make([][]byte, N)
	scanner := NewScanner(buf)
	for scanner.Scan() {
		data := Data(scanner.Bytes())
		require.NotEmpty(t, data)
		band := data[0]
		outputs[band] = append(outputs[band], data[1:]...)
	}

	require.NoError(t, scanner.Err())

	require.Equal(t, inputs, outputs)
}

func TestEachSidebandPacket(t *testing.T) {
	callbackError := errors.New("callback failed")

	testCases := []struct {
		desc     string
		in       string
		out      map[byte]string
		err      error
		callback func(byte, []byte) error
	}{
		{
			desc: "empty",
			out:  map[byte]string{},
		},
		{
			desc:     "empty with failing callback: callback does not run",
			out:      map[byte]string{},
			callback: func(byte, []byte) error { panic("oh no") },
		},
		{
			desc: "valid stream",
			in:   "0008\x00foo0008\x01bar0008\xfequx0008\xffbaz",
			out:  map[byte]string{0: "foo", 1: "bar", 254: "qux", 255: "baz"},
		},
		{
			desc:     "valid stream, failing callback",
			in:       "0008\x00foo0008\x01bar0008\xfequx0008\xffbaz",
			callback: func(byte, []byte) error { return callbackError },
			err:      callbackError,
		},
		{
			desc: "interrupted stream",
			in:   "ffff\x10hello world!!",
			err:  io.ErrUnexpectedEOF,
		},
		{
			desc: "stream without band",
			in:   "0004",
			err:  &errNotSideband{pkt: "0004"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			out := make(map[byte]string)
			callback := tc.callback
			if callback == nil {
				callback = func(band byte, data []byte) error {
					out[band] += string(data)
					return nil
				}
			}

			err := EachSidebandPacket(strings.NewReader(tc.in), callback)
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)

			if tc.callback == nil {
				require.Equal(t, tc.out, out)
			}
		})
	}
}
