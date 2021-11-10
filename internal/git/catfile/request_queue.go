package catfile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

type requestQueue struct {
	// isObjectQueue is set to `true` when this is a request queue which can be used for reading
	// objects. If set to `false`, then this can only be used to read object info.
	isObjectQueue bool

	stdout *bufio.Reader
	stdin  io.Writer

	// outstandingRequests is the number of requests which have been queued up. Gets incremented
	// on request, and decremented when starting to read an object (not when that object has
	// been fully consumed).
	outstandingRequests int64

	// closed indicates whether the queue is closed for additional requests.
	closed int32

	// currentObject is the currently read object.
	currentObject     *Object
	currentObjectLock sync.Mutex

	// trace is the current tracing span.
	trace *trace
}

// isDirty returns true either if there are outstanding requests for objects or if the current
// object hasn't yet been fully consumed.
func (q *requestQueue) isDirty() bool {
	q.currentObjectLock.Lock()
	defer q.currentObjectLock.Unlock()

	// We must check for the current object first: we cannot queue another object due to the
	// object lock, but we may queue another request while checking for dirtiness.
	if q.currentObject != nil {
		return q.currentObject.isDirty()
	}

	if atomic.LoadInt64(&q.outstandingRequests) != 0 {
		return true
	}

	return false
}

func (q *requestQueue) isClosed() bool {
	return atomic.LoadInt32(&q.closed) == 1
}

func (q *requestQueue) close() {
	if atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		q.currentObjectLock.Lock()
		defer q.currentObjectLock.Unlock()

		if q.currentObject != nil {
			q.currentObject.close()
		}
	}
}

func (q *requestQueue) RequestRevision(revision git.Revision) error {
	if q.isClosed() {
		return fmt.Errorf("cannot request revision: %w", os.ErrClosed)
	}

	atomic.AddInt64(&q.outstandingRequests, 1)

	if _, err := fmt.Fprintln(q.stdin, revision.String()); err != nil {
		atomic.AddInt64(&q.outstandingRequests, -1)
		return fmt.Errorf("requesting revision: %w", err)
	}

	return nil
}

func (q *requestQueue) ReadObject() (*Object, error) {
	if !q.isObjectQueue {
		panic("object queue used to read object info")
	}

	q.currentObjectLock.Lock()
	defer q.currentObjectLock.Unlock()

	if q.currentObject != nil {
		// If the current object is still dirty, then we must not try to read a new object.
		if q.currentObject.isDirty() {
			return nil, fmt.Errorf("current object has not been fully read")
		}

		q.currentObject.close()
		q.currentObject = nil

		// If we have already read an object before, then we must consume the trailing
		// newline after the object's data.
		if _, err := q.stdout.ReadByte(); err != nil {
			return nil, err
		}
	}

	objectInfo, err := q.readInfo()
	if err != nil {
		return nil, err
	}
	q.trace.recordRequest(objectInfo.Type)

	q.currentObject = &Object{
		ObjectInfo: *objectInfo,
		dataReader: io.LimitedReader{
			R: q.stdout,
			N: objectInfo.Size,
		},
		bytesRemaining: objectInfo.Size,
	}

	return q.currentObject, nil
}

func (q *requestQueue) ReadInfo() (*ObjectInfo, error) {
	if q.isObjectQueue {
		panic("object queue used to read object info")
	}

	objectInfo, err := q.readInfo()
	if err != nil {
		return nil, err
	}
	q.trace.recordRequest("info")

	return objectInfo, nil
}

func (q *requestQueue) readInfo() (*ObjectInfo, error) {
	if q.isClosed() {
		return nil, fmt.Errorf("cannot read object info: %w", os.ErrClosed)
	}

	// We first need to determine wether there are any queued requests at all. If not, then we
	// cannot read anything.
	queuedRequests := atomic.LoadInt64(&q.outstandingRequests)
	if queuedRequests == 0 {
		return nil, fmt.Errorf("no outstanding request")
	}

	// And when there are, we need to remove one of these queued requests. We do so via
	// `CompareAndSwapInt64()`, which easily allows us to detect concurrent access to the queue.
	if !atomic.CompareAndSwapInt64(&q.outstandingRequests, queuedRequests, queuedRequests-1) {
		return nil, fmt.Errorf("concurrent access to request queue")
	}

	return ParseObjectInfo(q.stdout)
}
