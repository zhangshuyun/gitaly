package streamcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// Pipes
//
//            +-------+
//            |       | <- *pipeReader <- Read()
// Write() -> | *pipe |       |
//            |       | <- *pipeReader <- Read()
//            +-------+       |  |
//                |           |  |
//                v           |  |
//           +----------+     |  |
//           |          | <---+  |
//           |   file   |        |
//           |          | <------+
//           +----------+
//
// Pipes are called so because their interface and behavior somewhat
// resembles Unix pipes, except there are multiple readers as opposed to
// just one. Just like with Unix pipes, pipe readers exert backpressure
// on the writer. When the write end is closed, the readers see EOF, just
// like they would when reading a file. When the read end is closed
// before the writer is done, the writer receives an error. This is all
// like it is with Unix pipes.
//
// The differences are as follows. When you create a pipe you get a write
// end and a read end, just like with Unix pipes. But now you can open an
// additional reader by calling OpenReader on the write end (the *pipe
// instance). Under the covers, a Unix pipe is just a buffer, and you
// cannot "rewind" it. With our pipe, there is an underlying file, so a
// new reader starts at position 0 in the file. It can then catch up with
// the other readers.
//
// Backpressure is implemented using two "cursors", wcursor and rcursor.
// A cursor is an integer value with a list of subscribers (see cursor.go).
// Every time the value goes up, the subscribers get a notification.
// Everytime the writer makes progress it increases the wcursor value
// so that it reflects the total number of bytes written to the file so
// far. The readers are all subscribed to wcursor so they get notified
// when there is new data for them to read. Conversely, the readers
// update the rcursor value after each read. Because of the way the
// cursor datatype works, the rcursor counter reflects the file position of
// the fastest reader. The writer is the sole subscriber of rcursor.
// Before each write, the writer checks the rcursor counter to make sure
// the writer itself is not ahead of its fastest reader.
//
// The wcursor cursor also serves second purpose. Besides notifying the
// readers when the writer writes new data, we also use it to check if
// there are any readers at all: if the subscriber list of wcursor is
// empty then there are no more readers. Each time a reader is closed,
// its subscription is removed from the wcursor subscriber list. If the
// list becomes empty before the writer is done, the writer fails with an
// error and the pipe is marked as "broken". This prevents us from
// writing data to disk that no one will read.

type pipe struct {
	m sync.Mutex

	// Access to underlying file
	name string
	w    io.WriteCloser

	// Reader/writer coordination. If wcursor > rcursor, the writer blocks
	// (back pressure). If rcursor >= wcursor, the readers block (waiting for
	// new data).
	wcursor *cursor
	rcursor *cursor

	// wnotifier is the channel the writer uses to wait for reader progress
	// notifications.
	wnotifier *notifier
}

func newPipe(w namedWriteCloser) (*pipeReader, *pipe, error) {
	p := &pipe{
		name:    w.Name(),
		w:       w,
		wcursor: newCursor(),
		rcursor: newCursor(),
	}
	p.wnotifier = p.rcursor.Subscribe()

	pr, err := p.OpenReader()
	if err != nil {
		return nil, nil, err
	}

	return pr, p, nil
}

func (p *pipe) Write(b []byte) (int, error) {
	// Loop (block) until at least one reader catches up with our last write.
	for p.wcursor.Position() > p.rcursor.Position() {
		select {
		case <-p.wcursor.Done():
			// Prevent writing bytes no-one will read
			return 0, errWrongCloseOrder
		case <-p.wnotifier.C:
		}
	}

	n, err := p.w.Write(b)

	// Notify blocked readers, if any, of new data that is available.
	p.wcursor.SetPosition(p.wcursor.Position() + int64(n))

	return n, err
}

var (
	errWrongCloseOrder     = errors.New("streamcache.pipe: all readers closed before writer finished")
	errWriterAlreadyClosed = errors.New("streamcache.pipe: writer already closed")
)

func (p *pipe) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	errClose := p.w.Close()

	if p.rcursor.IsDone() {
		return errWriterAlreadyClosed
	}

	if p.wcursor.IsDone() {
		return errWrongCloseOrder
	}

	// After this, p.rcursor.IsDone() will return true.
	p.rcursor.Unsubscribe(p.wnotifier)

	return errClose
}

func (p *pipe) RemoveFile() error { return os.Remove(p.name) }

func (p *pipe) OpenReader() (*pipeReader, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.wcursor.IsDone() && !p.rcursor.IsDone() {
		return nil, errWrongCloseOrder
	}

	r, err := os.Open(p.name)
	if err != nil {
		return nil, fmt.Errorf("OpenReader: %w", err)
	}

	pr := &pipeReader{
		pipe:     p,
		reader:   r,
		notifier: p.wcursor.Subscribe(),
	}

	return pr, nil
}

func (p *pipe) closeReader(pr *pipeReader) {
	p.m.Lock()
	defer p.m.Unlock()

	// Even though wcursor has its own embedded lock, we need to hold the
	// pipe lock when modifying it. This is because p.wcursor and p.rcursor
	// interact (see Close()).
	p.wcursor.Unsubscribe(pr.notifier)
}

type pipeReader struct {
	pipe     *pipe
	reader   io.ReadCloser
	position int64
	notifier *notifier

	// golangci-lint does not like this struct field because it is only used
	// on Linux. On macOS, it complains the field is unused. On Linux, it
	// complains that "nolint:unused" is unused. So we need "unused" and "structcheck" for
	// platforms other than Linux, and "nolintlint" for Linux.
	sendfileCalledSuccessfully bool //nolint:structcheck,unused,nolintlint
}

func (pr *pipeReader) Close() error {
	pr.pipe.closeReader(pr)
	return pr.reader.Close()
}

func (pr *pipeReader) waitReadable() bool {
	// Block until there is data for us to read. Note that it can actually
	// happen that pr.position > pr.pipe.wcursor, so we really want >= here, not
	// ==. There is a race between the moment the write end finishes writing
	// a chunk of data to the file and the moment pr.pipe.wcursor gets
	// updated.
wait:
	for pr.position >= pr.pipe.wcursor.Position() {
		select {
		case <-pr.pipe.rcursor.Done():
			break wait
		case <-pr.notifier.C:
		}
	}

	return pr.position < pr.pipe.wcursor.Position()
}

func (pr *pipeReader) advancePosition(n int) {
	pr.position += int64(n)

	// The writer is subscribed to changes in pr.pipe.rcursor. If it is
	// currently blocked, this call to SetPosition() will unblock it.
	pr.pipe.rcursor.SetPosition(pr.position)
}

func (pr *pipeReader) Read(b []byte) (int, error) {
	pr.waitReadable()
	n, err := pr.reader.Read(b)
	pr.advancePosition(n)
	return n, err
}
