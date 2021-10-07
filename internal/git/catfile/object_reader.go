package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// Object represents data returned by `git cat-file --batch`
type Object struct {
	// ObjectInfo represents main information about object
	ObjectInfo
	// Reader provides raw data about object. It differs for each type of object(tag, commit, tree, log, etc.)
	io.Reader
}

// ObjectReader is a reader for Git objects.
type ObjectReader interface {
	cacheable

	// Reader returns a new Object for the given revision. The Object must be fully consumed
	// before another object is requested.
	Object(_ context.Context, _ git.Revision) (*Object, error)
}

// objectReader is a reader for Git objects. Reading is implemented via a long-lived `git cat-file
// --batch` process such that we do not have to spawn a new process for each object we are about to
// read.
type objectReader struct {
	cmd    *command.Command
	stdout *bufio.Reader

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

	closed bool

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

	batchCmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch"},
			},
		},
		git.WithStdin(command.SetupStdin),
	)
	if err != nil {
		return nil, err
	}

	objectReader := &objectReader{
		cmd:         batchCmd,
		stdout:      bufio.NewReader(batchCmd),
		creationCtx: ctx,
		counter:     counter,
	}
	go func() {
		<-ctx.Done()
		objectReader.close()
		span.Finish()
	}()

	return objectReader, nil
}

func (o *objectReader) close() {
	o.Lock()
	defer o.Unlock()

	_ = o.cmd.Wait()

	o.closed = true
}

func (o *objectReader) isClosed() bool {
	o.Lock()
	defer o.Unlock()
	return o.closed
}

func (o *objectReader) Object(
	ctx context.Context,
	revision git.Revision,
) (*Object, error) {
	finish := startSpan(o.creationCtx, ctx, "Batch.Object", revision)
	defer finish()

	o.Lock()
	defer o.Unlock()

	if o.n == 1 {
		// Consume linefeed
		if _, err := o.stdout.ReadByte(); err != nil {
			return nil, err
		}
		o.n--
	}

	if o.n != 0 {
		return nil, fmt.Errorf("cannot create new Object: batch contains %d unread bytes", o.n)
	}

	if _, err := fmt.Fprintln(o.cmd, revision.String()); err != nil {
		return nil, err
	}

	oi, err := ParseObjectInfo(o.stdout)
	if err != nil {
		return nil, err
	}

	if o.counter != nil {
		o.counter.WithLabelValues(oi.Type).Inc()
	}

	o.n = oi.Size + 1

	return &Object{
		ObjectInfo: *oi,
		Reader: &objectDataReader{
			objectReader: o,
			r:            io.LimitReader(o.stdout, oi.Size),
		},
	}, nil
}

func (o *objectReader) consume(nBytes int) {
	o.n -= int64(nBytes)
	if o.n < 1 {
		panic("too many bytes read from batch")
	}
}

func (o *objectReader) isDirty() bool {
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

	if o.closed {
		return 0, os.ErrClosed
	}

	n, err := o.r.Read(p)
	o.objectReader.consume(n)
	return n, err
}
