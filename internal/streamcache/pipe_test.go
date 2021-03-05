package streamcache

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func createPipe(t *testing.T) (io.ReadCloser, *pipe, func()) {
	t.Helper()

	f, err := ioutil.TempFile("", "gitaly-streamcache-test")
	require.NoError(t, err)

	pr, p, err := newPipe(f)
	require.NoError(t, err)

	return pr, p, func() {
		_ = p.RemoveFile()
		p.Close()
	}
}

func writeBytes(w io.WriteCloser, buf []byte, progress *int64) error {
	for i := 0; i < len(buf); i++ {
		n, err := w.Write(buf[i : i+1])
		if err != nil {
			return err
		}
		if n != 1 {
			return io.ErrShortWrite
		}
		if progress != nil {
			atomic.AddInt64(progress, int64(n))
		}
	}
	return w.Close()
}

func TestPipe(t *testing.T) {
	pr, p, clean := createPipe(t)
	defer clean()

	readers := []io.ReadCloser{pr}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	const N = 10
	for len(readers) < N {
		r, err := p.OpenReader()
		require.NoError(t, err)
		readers = append(readers, r)
	}

	output := make([]bytes.Buffer, N)
	outErrors := make([]error, N)
	wg := &sync.WaitGroup{}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, outErrors[i] = io.Copy(&output[i], readers[i])
		}(i)
	}

	input := make([]byte, 4096)
	n, err := rand.Read(input)
	require.NoError(t, err)
	require.Equal(t, len(input), n)
	require.NoError(t, writeBytes(p, input, nil)) // write input only once

	wg.Wait()

	// Check that we read back the input N times
	for i := 0; i < N; i++ {
		require.Equal(t, input, output[i].Bytes())
		require.NoError(t, outErrors[i])
	}
}

func TestPipe_readAfterClose(t *testing.T) {
	pr1, p, clean := createPipe(t)
	defer clean()
	defer pr1.Close()
	defer p.Close()

	input := "hello world"
	werr := make(chan error, 1)
	go func() { werr <- writeBytes(p, []byte(input), nil) }()

	out1, err := ioutil.ReadAll(pr1)
	require.NoError(t, err)
	require.Equal(t, input, string(out1))

	require.NoError(t, <-werr)
	require.Equal(t, errWriterAlreadyClosed, p.Close(), "write end should already have been closed")

	pr2, err := p.OpenReader()
	require.NoError(t, err)
	defer pr2.Close()

	out2, err := ioutil.ReadAll(pr2)
	require.NoError(t, err)
	require.Equal(t, input, string(out2))
}

func TestPipe_backpressure(t *testing.T) {
	pr, p, clean := createPipe(t)
	defer clean()
	defer p.Close()
	defer pr.Close()

	input := "hello world"
	werr := make(chan error, 1)
	var wprogress int64
	go func() { werr <- writeBytes(p, []byte(input), &wprogress) }()

	var output []byte

	buf := make([]byte, 1)

	_, err := io.ReadFull(pr, buf)
	require.NoError(t, err)
	output = append(output, buf...)
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, int64(2), atomic.LoadInt64(&wprogress), "writer should be blocked after 2 bytes")

	_, err = io.ReadFull(pr, buf)
	require.NoError(t, err)
	output = append(output, buf...)
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, int64(3), atomic.LoadInt64(&wprogress), "writer should be blocked after having advanced 1 byte")

	rest, err := ioutil.ReadAll(pr)
	require.NoError(t, err)
	output = append(output, rest...)
	require.Equal(t, input, string(output))

	require.NoError(t, <-werr, "writer should have been unblocked and should have finished")
}

func TestPipe_closeWhenAllReadersLeave(t *testing.T) {
	pr1, p, clean := createPipe(t)
	defer clean()
	defer p.Close()
	defer pr1.Close()

	werr := make(chan error, 1)
	go func() { werr <- writeBytes(p, []byte("hello world"), nil) }()

	pr2, err := p.OpenReader()
	require.NoError(t, err)
	defer pr2.Close()

	// Sanity check
	select {
	case <-werr:
		t.Fatal("writer should still be blocked")
	default:
	}

	require.NoError(t, pr1.Close())
	time.Sleep(1 * time.Millisecond)

	select {
	case <-werr:
		t.Fatal("writer should still be blocked because there is still an active reader")
	default:
	}

	buf := make([]byte, 1)
	_, err = io.ReadFull(pr2, buf)
	require.NoError(t, err)
	require.Equal(t, "h", string(buf))

	require.NoError(t, pr2.Close())
	time.Sleep(1 * time.Millisecond)

	require.Error(t, <-werr, "writer should see error if all readers close before writer is done")
}

// Closing the last reader _before_ closing the writer is a failure
// condition. After this happens, opening new readers should fail.
func TestPipe_closeWrongOrder(t *testing.T) {
	pr, p, clean := createPipe(t)
	defer clean()
	defer p.Close()
	defer pr.Close()

	require.NoError(t, pr.Close(), "close last reader")
	require.Equal(t, errWrongCloseOrder, p.Close(), "closing writer should fail if all readers went away")

	_, err := p.OpenReader()
	require.Equal(t, errWrongCloseOrder, err, "opening reader after 'broken close' should fail")
}

// Closing last reader after closing the writer is the happy path. After
// this happens, opening new readers should work.
func TestPipe_closeOrderHappy(t *testing.T) {
	pr1, p, clean := createPipe(t)
	defer clean()
	defer p.Close()
	defer pr1.Close()

	require.NoError(t, p.Close())

	out1, err := ioutil.ReadAll(pr1)
	require.NoError(t, err)
	require.Empty(t, out1)

	pr2, err := p.OpenReader()
	require.NoError(t, err, "opening reader after normal close should succeed")
	defer pr2.Close()

	out2, err := ioutil.ReadAll(pr2)
	require.NoError(t, err)
	require.Empty(t, out2)
}

func TestPipe_concurrency(t *testing.T) {
	pr, p, clean := createPipe(t)
	defer clean()
	defer p.Close()
	defer pr.Close()

	const N = 100

	wg := &sync.WaitGroup{}

	// Prime N new goroutines that will open a reader
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start

			// This OpenReader call may fail, depending on the order pr and p get
			// closed, and whether the current goroutine runs before or after pr and
			// p get closed.
			if pr, err := p.OpenReader(); err == nil {
				_ = pr.Close()
			}
		}(i)
	}

	for _, c := range []io.Closer{pr, p} {
		wg.Add(1)

		go func(c io.Closer) {
			defer wg.Done()
			<-start

			// If pr closes before p, all subsequent calls to p.OpenReader() will
			// fail. If p closes before pr, all subsequent calls to p.OpenReader()
			// will succeed. We cannot predict which which happen here.
			_ = c.Close()
		}(c)
	}

	// Now we have 1 pipe with 1 reader. When we close start, both of them
	// will close, and at the same time N new readers will try to open.
	close(start)

	wg.Wait()
}
