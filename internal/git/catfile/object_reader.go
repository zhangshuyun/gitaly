package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
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
	cmd    *command.Command
	stdout *bufio.Reader

	// Even though the batch type should not be used concurrently, I think
	// that if that does happen by mistake we should give proper errors
	// instead of doing unsafe memory writes (to n) and failing in some
	// unpredictable way.
	sync.Mutex

	closed bool

	counter *prometheus.CounterVec

	// currentObject tracks the object that is currently being read.
	currentObject *Object
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
		stdout:  bufio.NewReader(batchCmd),
		counter: counter,
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
	if o.currentObject != nil {
		o.currentObject.close()
	}
}

func (o *objectReader) isClosed() bool {
	o.Lock()
	defer o.Unlock()
	return o.closed
}

func (o *objectReader) isDirty() bool {
	o.Lock()
	defer o.Unlock()

	if o.currentObject != nil {
		return o.currentObject.isDirty()
	}

	return false
}

func (o *objectReader) Object(
	ctx context.Context,
	revision git.Revision,
) (*Object, error) {
	trace := startTrace(ctx, o.counter, "catfile.Object")
	defer trace.finish()

	o.Lock()
	defer o.Unlock()

	if o.closed {
		return nil, fmt.Errorf("cannot read object: %w", os.ErrClosed)
	}

	if o.currentObject != nil {
		// If the current object is still dirty, then we must not try to read a new object.
		if o.currentObject.isDirty() {
			return nil, fmt.Errorf("current object has not been fully read")
		}

		o.currentObject.close()
		o.currentObject = nil

		// If we have already read an object before, then we must consume the trailing
		// newline after the object's data.
		if _, err := o.stdout.ReadByte(); err != nil {
			return nil, err
		}
	}

	if _, err := fmt.Fprintln(o.cmd, revision.String()); err != nil {
		return nil, err
	}

	oi, err := ParseObjectInfo(o.stdout)
	if err != nil {
		return nil, err
	}
	trace.recordRequest(oi.Type)

	o.currentObject = &Object{
		ObjectInfo: *oi,
		dataReader: io.LimitedReader{
			R: o.stdout,
			N: oi.Size,
		},
		bytesRemaining: oi.Size,
	}

	return o.currentObject, nil
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
