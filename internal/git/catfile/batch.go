package catfile

import (
	"context"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
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

func (bc *BatchCache) newBatch(ctx context.Context, repo git.RepositoryExecutor) (_ *batch, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	batchProcess, err := bc.newBatchProcess(ctx, repo)
	if err != nil {
		return nil, err
	}

	batchCheckProcess, err := bc.newBatchCheckProcess(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &batch{batchProcess: batchProcess, batchCheckProcess: batchCheckProcess}, nil
}

func newInstrumentedBatch(c Batch, catfileLookupCounter *prometheus.CounterVec) Batch {
	return &instrumentedBatch{c, catfileLookupCounter}
}

type instrumentedBatch struct {
	Batch
	catfileLookupCounter *prometheus.CounterVec
}

func (ib *instrumentedBatch) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Batch.Info", opentracing.Tag{"revision", revision})
	defer span.Finish()

	ib.catfileLookupCounter.WithLabelValues("info").Inc()

	return ib.Batch.Info(ctx, revision)
}

func (ib *instrumentedBatch) Tree(ctx context.Context, revision git.Revision) (*Object, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Batch.Tree", opentracing.Tag{"revision", revision})
	defer span.Finish()

	ib.catfileLookupCounter.WithLabelValues("tree").Inc()

	return ib.Batch.Tree(ctx, revision)
}

func (ib *instrumentedBatch) Commit(ctx context.Context, revision git.Revision) (*Object, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Batch.Commit", opentracing.Tag{"revision", revision})
	defer span.Finish()

	ib.catfileLookupCounter.WithLabelValues("commit").Inc()

	return ib.Batch.Commit(ctx, revision)
}

func (ib *instrumentedBatch) Blob(ctx context.Context, revision git.Revision) (*Object, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Batch.Blob", opentracing.Tag{"revision", revision})
	defer span.Finish()

	ib.catfileLookupCounter.WithLabelValues("blob").Inc()

	return ib.Batch.Blob(ctx, revision)
}

func (ib *instrumentedBatch) Tag(ctx context.Context, revision git.Revision) (*Object, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Batch.Tag", opentracing.Tag{"revision", revision})
	defer span.Finish()

	ib.catfileLookupCounter.WithLabelValues("tag").Inc()

	return ib.Batch.Tag(ctx, revision)
}
