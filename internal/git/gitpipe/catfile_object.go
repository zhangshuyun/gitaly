package gitpipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
)

// CatfileObjectResult is a result for the CatfileObject pipeline step.
type CatfileObjectResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

	// ObjectName is the object name as received from the revlistResultChan.
	ObjectName []byte
	// Object is the object returned by the CatfileObject pipeline step. The object must
	// be fully consumed.
	git.Object
}

type catfileObjectRequest struct {
	objectName []byte
	err        error
}

// CatfileObject processes catfileInfoResults from the given channel and reads associated objects
// into memory via `git cat-file --batch`. The returned channel will contain all processed objects.
// Any error received via the channel or encountered in this step will cause the pipeline to fail.
// Context cancellation will gracefully halt the pipeline. The returned object readers must always
// be fully consumed by the caller.
func CatfileObject(
	ctx context.Context,
	objectReader catfile.ObjectReader,
	it ObjectIterator,
) (CatfileObjectIterator, error) {
	queue, cleanup, err := objectReader.ObjectQueue(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	requestChan := make(chan catfileObjectRequest, 32)
	go func() {
		defer close(requestChan)

		var i int64
		for it.Next() {
			if err := queue.RequestRevision(it.ObjectID().Revision()); err != nil {
				select {
				case requestChan <- catfileObjectRequest{err: err}:
				case <-ctx.Done():
					return
				}
			}

			select {
			case requestChan <- catfileObjectRequest{objectName: it.ObjectName()}:
			case <-ctx.Done():
				return
			}

			i++
			if i%int64(cap(requestChan)) == 0 {
				if err := queue.Flush(); err != nil {
					select {
					case requestChan <- catfileObjectRequest{err: err}:
					case <-ctx.Done():
						return
					}
				}
			}
		}

		if err := it.Err(); err != nil {
			select {
			case requestChan <- catfileObjectRequest{err: err}:
			case <-ctx.Done():
				return
			}
		}

		if err := queue.Flush(); err != nil {
			select {
			case requestChan <- catfileObjectRequest{err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	resultChan := make(chan CatfileObjectResult)
	go func() {
		defer close(resultChan)

		sendResult := func(result CatfileObjectResult) bool {
			// In case the context has been cancelled, we have a race between observing
			// an error from the killed Git process and observing the context
			// cancellation itself. But if we end up here because of cancellation of the
			// Git process, we don't want to pass that one down the pipeline but instead
			// just stop the pipeline gracefully. We thus have this check here up front
			// to error messages from the Git process.
			select {
			case <-ctx.Done():
				return true
			default:
			}

			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		var previousObject *synchronizingObject

		// It's fine to iterate over the request channel without paying attention to
		// context cancellation because the request channel itself would be closed if the
		// context was cancelled.
		for request := range requestChan {
			if request.err != nil {
				sendResult(CatfileObjectResult{err: request.err})
				break
			}

			// We mustn't try to read another object before reading the previous object
			// has concluded. Given that this is not under our control but under the
			// control of the caller, we thus have to wait until the blocking reader has
			// reached EOF.
			if previousObject != nil {
				select {
				case <-previousObject.doneCh:
				case <-ctx.Done():
					return
				}
			}

			object, err := queue.ReadObject()
			if err != nil {
				sendResult(CatfileObjectResult{
					err: fmt.Errorf("requesting object: %w", err),
				})
				return
			}

			previousObject = &synchronizingObject{
				Object: object,
				doneCh: make(chan interface{}),
			}

			if isDone := sendResult(CatfileObjectResult{
				ObjectName: request.objectName,
				Object:     previousObject,
			}); isDone {
				return
			}
		}
	}()

	return &catfileObjectIterator{
		ch: resultChan,
	}, nil
}

type synchronizingObject struct {
	git.Object

	doneCh    chan interface{}
	closeOnce sync.Once
}

func (r *synchronizingObject) Read(p []byte) (int, error) {
	n, err := r.Object.Read(p)
	if errors.Is(err, io.EOF) {
		r.closeOnce.Do(func() {
			close(r.doneCh)
		})
	}
	return n, err
}

func (r *synchronizingObject) WriteTo(w io.Writer) (int64, error) {
	n, err := r.Object.WriteTo(w)
	r.closeOnce.Do(func() {
		close(r.doneCh)
	})
	return n, err
}
