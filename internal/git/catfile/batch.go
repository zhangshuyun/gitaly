package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/git"
)

// batch encapsulates a 'git cat-file --batch' process
type batch struct {
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

func newBatch(ctx context.Context, repoPath string, env []string) (*batch, error) {
	b := &batch{}

	var stdinReader io.Reader
	stdinReader, b.w = io.Pipe()
	batchCmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch"}
	batchCmd, err := git.BareCommand(ctx, stdinReader, nil, nil, env, batchCmdArgs...)
	if err != nil {
		return nil, err
	}
	b.r = bufio.NewReader(batchCmd)
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		b.w.Close()
	}()

	return b, nil
}

func (b *batch) reader(revspec string, expectedType string) (io.Reader, error) {
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
		return nil, fmt.Errorf("cannot create new reader: batch contains %d unread bytes", b.n)
	}

	if _, err := fmt.Fprintln(b.w, revspec); err != nil {
		return nil, err
	}

	oi, err := parseObjectInfo(b.r)
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

		return nil, fmt.Errorf("expected %s to be a %s, got %s", oi.Oid, expectedType, oi.Type)
	}

	return &batchReader{
		batch: b,
		r:     io.LimitReader(b.r, oi.Size),
	}, nil
}

func (b *batch) consume(nBytes int) {
	b.Lock()
	defer b.Unlock()

	b.n -= int64(nBytes)
	if b.n < 1 {
		panic("too many bytes read from batch")
	}
}

type batchReader struct {
	*batch
	r io.Reader
}

func (br *batchReader) Read(p []byte) (int, error) {
	n, err := br.r.Read(p)
	br.batch.consume(n)
	return n, err
}
