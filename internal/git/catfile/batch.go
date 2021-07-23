package catfile

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// SessionIDField is the gRPC metadata field we use to store the gitaly session ID.
	SessionIDField = "gitaly-session-id"
)

// Batch abstracts 'git cat-file --batch' and 'git cat-file --batch-check'.
// It lets you retrieve object metadata and raw objects from a Git repo.
//
// A Batch instance can only serve single request at a time. If you want to
// use it across multiple goroutines you need to add your own locking.
type Batch interface {
	Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error)
	Tree(ctx context.Context, revision git.Revision) (*Object, error)
	Commit(ctx context.Context, revision git.Revision) (*Object, error)
	Blob(ctx context.Context, revision git.Revision) (*Object, error)
	Tag(ctx context.Context, revision git.Revision) (*Object, error)
}

type batch struct {
	sync.Mutex
	*batchCheckProcess
	*batchProcess
	cancel func()
	closed bool
}

// Info returns an ObjectInfo if spec exists. If the revision does not exist
// the error is of type NotFoundError.
func (c *batch) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	return c.batchCheckProcess.info(revision)
}

// Tree returns a raw tree object. It is an error if the revision does not
// point to a tree. To prevent this, use Info to resolve the revision and check
// the object type. Caller must consume the Reader before making another call
// on C.
func (c *batch) Tree(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.batchProcess.reader(revision, "tree")
}

// Commit returns a raw commit object. It is an error if the revision does not
// point to a commit. To prevent this, use Info to resolve the revision and
// check the object type. Caller must consume the Reader before making another
// call on C.
func (c *batch) Commit(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.batchProcess.reader(revision, "commit")
}

// Blob returns a reader for the requested blob. The entire blob must be
// read before any new objects can be requested from this Batch instance.
//
// It is an error if the revision does not point to a blob. To prevent this,
// use Info to resolve the revision and check the object type.
func (c *batch) Blob(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.batchProcess.reader(revision, "blob")
}

// Tag returns a raw tag object. Caller must consume the Reader before
// making another call on C.
func (c *batch) Tag(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.batchProcess.reader(revision, "tag")
}

// Close closes the writers for batchCheckProcess and batchProcess. This is only used for cached
// Batches
func (c *batch) Close() {
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	if c.cancel != nil {
		// both c.batchProcess and c.batchCheckProcess have goroutines that listen on
		// ctx.Done() when this is cancelled, it will cause those goroutines to close both
		// writers
		c.cancel()
	}
}

func (c *batch) isClosed() bool {
	c.Lock()
	defer c.Unlock()
	return c.closed
}

type simulatedBatchSpawnError struct{}

func (simulatedBatchSpawnError) Error() string { return "simulated spawn error" }

func (bc *BatchCache) newBatch(ctx context.Context, repo git.RepositoryExecutor) (*batch, context.Context, error) {
	var err error

	// batch processes are long-lived and reused across RPCs,
	// so we de-correlate the process from the RPC
	ctx = correlation.ContextWithCorrelation(ctx, "")
	ctx = opentracing.ContextWithSpan(ctx, nil)
	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.Batch")

	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	go func() {
		<-ctx.Done()
		span.Finish()
	}()

	batchProcess, err := bc.newBatchProcess(ctx, repo)
	if err != nil {
		return nil, ctx, err
	}

	batchCheckProcess, err := bc.newBatchCheckProcess(ctx, repo)
	if err != nil {
		return nil, ctx, err
	}

	return &batch{batchProcess: batchProcess, batchCheckProcess: batchCheckProcess}, ctx, nil
}

func newInstrumentedBatch(ctx context.Context, c Batch, catfileLookupCounter *prometheus.CounterVec) Batch {
	return &instrumentedBatch{
		Batch:                c,
		catfileLookupCounter: catfileLookupCounter,
		batchCtx:             ctx,
	}
}

// We maintain two contexts here: the one RPC-level one, and one batch-level one.
//
// The batchCtx tracks the lifetime of the long-running batch process, and is
// de-correlated from the RPC, as it is shared between many RPCs.
//
// We perform double accounting and re-correlation to get stats and traces per
// batch process.
type instrumentedBatch struct {
	Batch
	catfileLookupCounter *prometheus.CounterVec
	batchCtx             context.Context
}

func (ib *instrumentedBatch) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	ctx, finish := ib.startSpan(ctx, "Batch.Info", revision)
	defer finish()

	ib.catfileLookupCounter.WithLabelValues("info").Inc()

	return ib.Batch.Info(ctx, revision)
}

func (ib *instrumentedBatch) Tree(ctx context.Context, revision git.Revision) (*Object, error) {
	ctx, finish := ib.startSpan(ctx, "Batch.Tree", revision)
	defer finish()

	ib.catfileLookupCounter.WithLabelValues("tree").Inc()

	return ib.Batch.Tree(ctx, revision)
}

func (ib *instrumentedBatch) Commit(ctx context.Context, revision git.Revision) (*Object, error) {
	ctx, finish := ib.startSpan(ctx, "Batch.Commit", revision)
	defer finish()

	ib.catfileLookupCounter.WithLabelValues("commit").Inc()

	return ib.Batch.Commit(ctx, revision)
}

func (ib *instrumentedBatch) Blob(ctx context.Context, revision git.Revision) (*Object, error) {
	ctx, finish := ib.startSpan(ctx, "Batch.Blob", revision)
	defer finish()

	ib.catfileLookupCounter.WithLabelValues("blob").Inc()

	return ib.Batch.Blob(ctx, revision)
}

func (ib *instrumentedBatch) Tag(ctx context.Context, revision git.Revision) (*Object, error) {
	ctx, finish := ib.startSpan(ctx, "Batch.Tag", revision)
	defer finish()

	ib.catfileLookupCounter.WithLabelValues("tag").Inc()

	return ib.Batch.Tag(ctx, revision)
}

func (ib *instrumentedBatch) revisionTag(revision git.Revision) opentracing.Tag {
	return opentracing.Tag{Key: "revision", Value: revision}
}

func (ib *instrumentedBatch) correlationIDTag(ctx context.Context) opentracing.Tag {
	return opentracing.Tag{Key: "correlation_id", Value: correlation.ExtractFromContext(ctx)}
}

func (ib *instrumentedBatch) startSpan(ctx context.Context, methodName string, revision git.Revision) (context.Context, func()) {
	span, ctx := opentracing.StartSpanFromContext(ctx, methodName, ib.revisionTag(revision))
	span2, _ := opentracing.StartSpanFromContext(ib.batchCtx, methodName, ib.revisionTag(revision), ib.correlationIDTag(ctx))

	return ctx, func() {
		span.Finish()
		span2.Finish()
	}
}
