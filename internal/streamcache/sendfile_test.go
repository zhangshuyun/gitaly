//go:build linux
// +build linux

package streamcache

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

type wrappedFile struct{ f *os.File }

func (wf *wrappedFile) Write(p []byte) (int, error) { return wf.f.Write(p) }
func (wf *wrappedFile) Close() error                { return wf.f.Close() }
func (wf *wrappedFile) Name() string                { return wf.f.Name() }

func TestPipe_WriteTo(t *testing.T) {
	data := make([]byte, 10*1024*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)

	testCases := []struct {
		desc     string
		create   func(t *testing.T) namedWriteCloser
		sendfile bool
	}{
		{
			desc: "os.File",
			create: func(t *testing.T) namedWriteCloser {
				f, err := ioutil.TempFile("", "pipe write to")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, os.Remove(f.Name())) })
				return f
			},
			sendfile: true,
		},
		{
			desc: "non-file writer",
			create: func(t *testing.T) namedWriteCloser {
				f, err := ioutil.TempFile("", "pipe write to")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, os.Remove(f.Name())) })
				return &wrappedFile{f}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pr, p := createPipe(t)
			defer pr.Close()

			errC := make(chan error, 1)
			go func() {
				errC <- func() error {
					defer p.Close()

					// To exercise pipe blocking logic, we want to prevent writing all of
					// data at once.
					r := iotest.HalfReader(bytes.NewReader(data))
					if _, err := io.Copy(p, r); err != nil {
						return err
					}
					return p.Close()
				}()
			}()

			outW := tc.create(t)
			require.NoError(t, err)
			defer outW.Close()

			n, err := pr.WriteTo(outW)
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), n)

			require.NoError(t, outW.Close())
			require.NoError(t, <-errC)

			outBytes, err := os.ReadFile(outW.Name())
			require.NoError(t, err)
			// Don't use require.Equal because we don't want a 10MB error message.
			require.True(t, bytes.Equal(data, outBytes))

			require.Equal(t, tc.sendfile, pr.sendfileCalledSuccessfully)
		})
	}
}

func TestPipe_WriteTo_EAGAIN(t *testing.T) {
	data := make([]byte, 10*1024*1024)
	_, err := rand.Read(data)
	require.NoError(t, err)

	pr, p := createPipe(t)
	defer pr.Close()
	defer p.Close()

	_, err = p.Write(data)
	require.NoError(t, err)
	require.NoError(t, p.Close())

	fr, fw, err := os.Pipe()
	require.NoError(t, err)
	defer fr.Close()
	defer fw.Close()

	errC := make(chan error, 1)
	go func() {
		errC <- func() error {
			defer fw.Close()

			// This will try to write 10MB into fw at once, which will fail because
			// the pipe buffer is too small. Then sendfile will return EAGAIN. Doing
			// this tests our ability to handle EAGAIN correctly.
			_, err := pr.WriteTo(fw)
			if err != nil {
				return err
			}

			return fw.Close()
		}()
	}()

	out, err := io.ReadAll(fr)
	require.NoError(t, err)
	// Don't use require.Equal because we don't want a 10MB error message.
	require.True(t, bytes.Equal(data, out))

	require.NoError(t, <-errC)
	require.True(t, pr.sendfileCalledSuccessfully)
}
