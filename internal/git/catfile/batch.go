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
	*objectInfoReader
	*objectReader

	closedMutex sync.Mutex
	closed      bool
}

// Info returns an ObjectInfo if spec exists. If the revision does not exist
// the error is of type NotFoundError.
func (c *batch) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	return c.objectInfoReader.info(ctx, revision)
}

// Tree returns a raw tree object. It is an error if the revision does not
// point to a tree. To prevent this, use Info to resolve the revision and check
// the object type. Caller must consume the Reader before making another call
// on C.
func (c *batch) Tree(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.objectReader.reader(ctx, revision, "tree")
}

// Commit returns a raw commit object. It is an error if the revision does not
// point to a commit. To prevent this, use Info to resolve the revision and
// check the object type. Caller must consume the Reader before making another
// call on C.
func (c *batch) Commit(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.objectReader.reader(ctx, revision, "commit")
}

// Blob returns a reader for the requested blob. The entire blob must be
// read before any new objects can be requested from this Batch instance.
//
// It is an error if the revision does not point to a blob. To prevent this,
// use Info to resolve the revision and check the object type.
func (c *batch) Blob(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.objectReader.reader(ctx, revision, "blob")
}

// Tag returns a raw tag object. Caller must consume the Reader before
// making another call on C.
func (c *batch) Tag(ctx context.Context, revision git.Revision) (*Object, error) {
	return c.objectReader.reader(ctx, revision, "tag")
}

// Close closes the writers for objectInfoReader and objectReader. This is only used for cached
// Batches
func (c *batch) Close() {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	c.objectReader.Close()
	c.objectInfoReader.Close()
}

func (c *batch) isClosed() bool {
	c.closedMutex.Lock()
	defer c.closedMutex.Unlock()
	return c.closed
}

func newBatch(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (_ *batch, returnedErr error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.Batch")
	go func() {
		<-ctx.Done()
		span.Finish()
	}()

	objectReader, err := newObjectReader(ctx, repo, counter)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If creation of the ObjectInfoReader fails, then we do not want to leak the
		// ObjectReader process.
		if returnedErr != nil {
			objectReader.Close()
		}
	}()

	objectInfoReader, err := newObjectInfoReader(ctx, repo, counter)
	if err != nil {
		return nil, err
	}

	return &batch{objectReader: objectReader, objectInfoReader: objectInfoReader}, nil
}
