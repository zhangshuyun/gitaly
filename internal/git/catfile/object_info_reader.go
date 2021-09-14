package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// objectInfoReader is a reader for Git object information. This reader is implemented via a
// long-lived  `git cat-file --batch-check` process such that we do not have to spawn a separate
// process per object info we're about to read.
type objectInfoReader struct {
	r *bufio.Reader
	w io.WriteCloser
	sync.Mutex

	// creationCtx is the context in which this reader has been created. This context may
	// potentially be decorrelated from the "real" RPC context in case the reader is going to be
	// cached.
	creationCtx context.Context
	counter     *prometheus.CounterVec
}

func newObjectInfoReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectInfoReader, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.ObjectInfoReader")

	stdinReader, stdinWriter := io.Pipe()
	batchCmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch-check"},
			},
		},
		git.WithStdin(stdinReader),
	)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		stdinWriter.Close()
		span.Finish()
	}()

	return &objectInfoReader{
		w:           stdinWriter,
		r:           bufio.NewReader(batchCmd),
		creationCtx: ctx,
		counter:     counter,
	}, nil
}

func (o *objectInfoReader) info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	finish := startSpan(o.creationCtx, ctx, "Batch.Info", revision)
	defer finish()

	if o.counter != nil {
		o.counter.WithLabelValues("info").Inc()
	}

	o.Lock()
	defer o.Unlock()

	if _, err := fmt.Fprintln(o.w, revision.String()); err != nil {
		return nil, err
	}

	return ParseObjectInfo(o.r)
}
