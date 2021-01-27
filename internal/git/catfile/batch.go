package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// batch encapsulates a 'git cat-file --batch' process
type batchProcess struct {
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
}

func newBatchProcess(ctx context.Context, gitCmdFactory git.CommandFactory, repo repository.GitRepo) (*batchProcess, error) {
	totalCatfileProcesses.Inc()
	b := &batchProcess{}

	var stdinReader io.Reader
	stdinReader, b.w = io.Pipe()

	// batch processes are long-lived and reused across RPCs,
	// so we de-correlate the process from the RPC
	ctx = correlation.ContextWithCorrelation(ctx, "")
	ctx = opentracing.ContextWithSpan(ctx, nil)

	batchCmd, err := gitCmdFactory.New(ctx, repo, nil,
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

	b.r = bufio.NewReader(batchCmd)

	currentCatfileProcesses.Inc()
	go func() {
		<-ctx.Done()
		// This Close() is crucial to prevent leaking file descriptors.
		b.w.Close()
		currentCatfileProcesses.Dec()
	}()

	if injectSpawnErrors {
		// Testing only: intentionally leak process
		return nil, &simulatedBatchSpawnError{}
	}

	return b, nil
}

func (b *batchProcess) reader(revision git.Revision, expectedType string) (*Object, error) {
	b.Lock()
	defer b.Unlock()

	if b.n == 1 {
		// Consume linefeed
		if _, err := b.r.ReadByte(); err != nil {
			return nil, err
		}
		b.n--
	}

	if b.n != 0 {
		return nil, fmt.Errorf("cannot create new Object: batch contains %d unread bytes", b.n)
	}

	if _, err := fmt.Fprintln(b.w, revision.String()); err != nil {
		return nil, err
	}

	oi, err := ParseObjectInfo(b.r)
	if err != nil {
		return nil, err
	}

	b.n = oi.Size + 1

	if oi.Type != expectedType {
		// This is a programmer error and it should never happen. But if it does,
		// we need to leave the cat-file process in a good state
		if _, err := io.CopyN(ioutil.Discard, b.r, b.n); err != nil {
			return nil, err
		}
		b.n = 0

		return nil, NotFoundError{error: fmt.Errorf("expected %s to be a %s, got %s", oi.Oid, expectedType, oi.Type)}
	}

	return &Object{
		ObjectInfo: *oi,
		Reader: &batchReader{
			batchProcess: b,
			r:            io.LimitReader(b.r, oi.Size),
		},
	}, nil
}

func (b *batchProcess) consume(nBytes int) {
	b.n -= int64(nBytes)
	if b.n < 1 {
		panic("too many bytes read from batch")
	}
}

func (b *batchProcess) hasUnreadData() bool {
	b.Lock()
	defer b.Unlock()

	return b.n > 1
}

type batchReader struct {
	*batchProcess
	r io.Reader
}

func (br *batchReader) Read(p []byte) (int, error) {
	br.batchProcess.Lock()
	defer br.batchProcess.Unlock()

	n, err := br.r.Read(p)
	br.batchProcess.consume(n)
	return n, err
}
