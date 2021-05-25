package inspect

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
)

// NewWriter returns Writer that will feed 'action' with data on each write to it.
// It is required to call Close once all data is processed.
// Close will be blocked until action is completed.
func NewWriter(writer io.Writer, action func(reader io.Reader)) io.WriteCloser {
	pr, pw := io.Pipe()

	multiOut := io.MultiWriter(writer, pw)
	c := closer{c: pw, done: make(chan struct{})}

	go func() {
		defer close(c.done) // channel close signals that action is completed

		action(pr)

		_, _ = io.Copy(ioutil.Discard, pr) // consume all data to unblock multiOut
	}()

	return struct {
		io.Writer
		io.Closer
	}{
		Writer: multiOut,
		Closer: c, // pw must be closed otherwise goroutine serving 'action' won't be terminated
	}
}

type closer struct {
	c    io.Closer
	done chan struct{}
}

// Close closes wrapped Closer and waits for done to be closed.
func (c closer) Close() error {
	defer func() { <-c.done }()
	return c.c.Close()
}

// LogPackInfoStatistic inspect data stream for the informational messages
// and logs info about pack file usage.
func LogPackInfoStatistic(ctx context.Context) func(reader io.Reader) {
	return func(reader io.Reader) {
		logger := ctxlogrus.Extract(ctx)

		scanner := pktline.NewScanner(reader)
		for scanner.Scan() {
			pktData := pktline.Data(scanner.Bytes())
			if !bytes.HasPrefix(pktData, []byte("\x02Total ")) {
				continue
			}

			logger.WithField("pack.stat", text.ChompBytes(pktData[1:])).Info("pack file compression statistic")
		}
		// we are not interested in scanner.Err()
	}
}
