package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

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
	cmd *command.Command

	counter *prometheus.CounterVec

	queue      requestQueue
	queueInUse int32
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
		cmd:     batchCmd,
		counter: counter,
		queue: requestQueue{
			isObjectQueue: true,
			stdout:        bufio.NewReader(batchCmd),
			stdin:         batchCmd,
		},
	}
	go func() {
		<-ctx.Done()
		objectReader.close()
		span.Finish()
	}()

	return objectReader, nil
}

func (o *objectReader) close() {
	o.queue.close()
	_ = o.cmd.Wait()
}

func (o *objectReader) isClosed() bool {
	return o.queue.isClosed()
}

func (o *objectReader) isDirty() bool {
	return o.queue.isDirty()
}

func (o *objectReader) objectQueue(ctx context.Context, tracedMethod string) (*requestQueue, func(), error) {
	if !atomic.CompareAndSwapInt32(&o.queueInUse, 0, 1) {
		return nil, nil, fmt.Errorf("object queue already in use")
	}

	trace := startTrace(ctx, o.counter, tracedMethod)
	o.queue.trace = trace

	return &o.queue, func() {
		atomic.StoreInt32(&o.queueInUse, 0)
		trace.finish()
	}, nil
}

func (o *objectReader) Object(ctx context.Context, revision git.Revision) (*Object, error) {
	queue, finish, err := o.objectQueue(ctx, "catfile.Object")
	if err != nil {
		return nil, err
	}
	defer finish()

	if err := queue.RequestRevision(revision); err != nil {
		return nil, err
	}

	object, err := queue.ReadObject()
	if err != nil {
		return nil, err
	}

	return object, nil
}

// Object represents data returned by `git cat-file --batch`
type Object struct {
	// ObjectInfo represents main information about object
	ObjectInfo

	// dataReader is reader which has all the object data.
	dataReader io.LimitedReader

	// bytesLeft tracks the number of bytes which are left to be read. While this duplicates the
	// information tracked in dataReader.N, this cannot be helped given that we need to make
	// access to this information atomic so there's no race between updating it and checking the
	// process for dirtiness. While we could use locking instead of atomics, we'd have to lock
	// during the whole read duration -- and thus it'd become impossible to check for dirtiness
	// at the same time.
	bytesRemaining int64

	// closed determines whether the object is closed for reading.
	closed int32
}

// isDirty determines whether the object is still dirty, that is whether there are still unconsumed
// bytes.
func (o *Object) isDirty() bool {
	return atomic.LoadInt64(&o.bytesRemaining) != 0
}

func (o *Object) isClosed() bool {
	return atomic.LoadInt32(&o.closed) == 1
}

func (o *Object) close() {
	atomic.StoreInt32(&o.closed, 1)
}

func (o *Object) Read(p []byte) (int, error) {
	if o.isClosed() {
		return 0, os.ErrClosed
	}

	n, err := o.dataReader.Read(p)
	if atomic.AddInt64(&o.bytesRemaining, int64(-n)) < 0 {
		return n, fmt.Errorf("bytes remaining became negative while reading object")
	}

	return n, err
}

// WriteTo implements the io.WriterTo interface. It defers the write to the embedded object reader
// via `io.Copy()`, which in turn will use `WriteTo()` or `ReadFrom()` in case these interfaces are
// implemented by the respective reader or writer.
func (o *Object) WriteTo(w io.Writer) (int64, error) {
	if o.isClosed() {
		return 0, os.ErrClosed
	}

	// While the `io.LimitedReader` does not support WriteTo, `io.Copy()` will make use of
	// `ReadFrom()` in case the writer implements it.
	n, err := io.Copy(w, &o.dataReader)
	if atomic.AddInt64(&o.bytesRemaining, -n) < 0 {
		return n, fmt.Errorf("bytes remaining became negative while reading object")
	}

	return n, err
}
