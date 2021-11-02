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

func (o *objectReader) consume(nBytes int64) {
	o.n -= nBytes
	if o.n < 1 {
		panic("too many bytes read from batch")
	}
}

func (o *objectReader) isDirty() bool {
	o.Lock()
	defer o.Unlock()

	return o.n > 1
}

// Object represents data returned by `git cat-file --batch`
type Object struct {
	// ObjectInfo represents main information about object
	ObjectInfo
	// parent is the objectReader which has created the Object.
	parent *objectReader
	// dataReader is reader which has all the object data.
	dataReader io.LimitedReader
}

func (o *objectReader) Object(
	ctx context.Context,
	revision git.Revision,
) (*Object, error) {
	trace, finish := startTrace(ctx, o.creationCtx, o.counter, "catfile.Object")
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
	trace.recordRequest(oi.Type)

	o.n = oi.Size + 1

	return &Object{
		ObjectInfo: *oi,
		parent:     o,
		dataReader: io.LimitedReader{
			R: o.stdout,
			N: oi.Size,
		},
	}, nil
}

func (o *Object) Read(p []byte) (int, error) {
	o.parent.Lock()
	defer o.parent.Unlock()

	if o.parent.closed {
		return 0, os.ErrClosed
	}

	n, err := o.dataReader.Read(p)
	o.parent.consume(int64(n))
	return n, err
}

// WriteTo implements the io.WriterTo interface. It defers the write to the embedded object reader
// via `io.Copy()`, which in turn will use `WriteTo()` or `ReadFrom()` in case these interfaces are
// implemented by the respective reader or writer.
func (o *Object) WriteTo(w io.Writer) (int64, error) {
	o.parent.Lock()
	defer o.parent.Unlock()

	if o.parent.closed {
		return 0, os.ErrClosed
	}

	// While the `io.LimitedReader` does not support WriteTo, `io.Copy()` will make use of
	// `ReadFrom()` in case the writer implements it.
	n, err := io.Copy(w, &o.dataReader)
	o.parent.consume(n)
	return n, err
}
