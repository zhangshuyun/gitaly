package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// objectReader is a reader for Git objects. Reading is implemented via a long-lived `git cat-file
// --batch` process such that we do not have to spawn a new process for each object we are about to
// read.
type objectReader struct {
	r *bufio.Reader
	w io.WriteCloser

	// n is a state machine that tracks how much data we still have to read
	// from r. Legal states are: n==0, this means we can do a new request on
	// the cat-file process. n==1, this means that we have to discard a
	// trailing newline. n>0, this means we are in the middle of reading a
	// raw git object.
	n int64

	// Even though the batch type should not be used concurrently, I think
	// that if that does happen by mistake we should give proper errors
	// instead of doing unsafe memory writes (to n) and failing in some
	// unpredictable way.
	sync.Mutex

	// creationCtx is the context in which this reader has been created. This context may
	// potentially be decorrelated from the "real" RPC context in case the reader is going to be
	// cached.
	creationCtx context.Context
	counter     *prometheus.CounterVec
}

func newObjectReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectReader, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.ObjectReader")

	stdinReader, stdinWriter := io.Pipe()
	batchCmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch"},
			},
		},
		git.WithStdin(stdinReader),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		// This Close() is crucial to prevent leaking file descriptors.
		stdinWriter.Close()
		span.Finish()
	}()

	return &objectReader{
		w:           stdinWriter,
		r:           bufio.NewReader(batchCmd),
		creationCtx: ctx,
		counter:     counter,
	}, nil
}

func (o *objectReader) reader(
	ctx context.Context,
	revision git.Revision,
	expectedType string,
) (*Object, error) {
	finish := startSpan(o.creationCtx, ctx, fmt.Sprintf("Batch.Object(%s)", expectedType), revision)
	defer finish()

	if o.counter != nil {
		o.counter.WithLabelValues(expectedType).Inc()
	}

	o.Lock()
	defer o.Unlock()

	if o.n == 1 {
		// Consume linefeed
		if _, err := o.r.ReadByte(); err != nil {
			return nil, err
		}
		o.n--
	}

	if o.n != 0 {
		return nil, fmt.Errorf("cannot create new Object: batch contains %d unread bytes", o.n)
	}

	if _, err := fmt.Fprintln(o.w, revision.String()); err != nil {
		return nil, err
	}

	oi, err := ParseObjectInfo(o.r)
	if err != nil {
		return nil, err
	}

	o.n = oi.Size + 1

	if oi.Type != expectedType {
		// This is a programmer error and it should never happen. But if it does,
		// we need to leave the cat-file process in a good state
		if _, err := io.CopyN(ioutil.Discard, o.r, o.n); err != nil {
			return nil, err
		}
		o.n = 0

		return nil, NotFoundError{error: fmt.Errorf("expected %s to be a %s, got %s", oi.Oid, expectedType, oi.Type)}
	}

	return &Object{
		ObjectInfo: *oi,
		Reader: &objectDataReader{
			objectReader: o,
			r:            io.LimitReader(o.r, oi.Size),
		},
	}, nil
}

func (o *objectReader) consume(nBytes int) {
	o.n -= int64(nBytes)
	if o.n < 1 {
		panic("too many bytes read from batch")
	}
}

func (o *objectReader) hasUnreadData() bool {
	o.Lock()
	defer o.Unlock()

	return o.n > 1
}

type objectDataReader struct {
	*objectReader
	r io.Reader
}

func (o *objectDataReader) Read(p []byte) (int, error) {
	o.objectReader.Lock()
	defer o.objectReader.Unlock()

	n, err := o.r.Read(p)
	o.objectReader.consume(n)
	return n, err
}
