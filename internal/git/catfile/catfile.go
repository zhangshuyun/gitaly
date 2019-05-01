package catfile

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
)

// Batch abstracts 'git cat-file --batch' and 'git cat-file --batch-check'.
// It lets you retrieve object metadata and raw objects from a Git repo.
//
// A Batch instance can only serve single request at a time. If you want to
// use it across multiple goroutines you need to add your own locking.
type Batch struct {
	*batchCheck
	*batch
}

// Info returns an ObjectInfo if spec exists. If spec does not exist the
// error is of type NotFoundError.
func (c *Batch) Info(revspec string) (*ObjectInfo, error) {
	return c.batchCheck.info(revspec)
}

// Tree returns a raw tree object. It is an error if revspec does not
// point to a tree. To prevent this firstuse Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tree(revspec string) (io.Reader, error) {
	return c.batch.reader(revspec, "tree")
}

// Commit returns a raw commit object. It is an error if revspec does not
// point to a commit. To prevent this first use Info to resolve the revspec
// and check the object type. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Commit(revspec string) (io.Reader, error) {
	return c.batch.reader(revspec, "commit")
}

// Blob returns a reader for the requested blob. The entire blob must be
// read before any new objects can be requested from this Batch instance.
//
// It is an error if revspec does not point to a blob. To prevent this
// first use Info to resolve the revspec and check the object type.
func (c *Batch) Blob(revspec string) (io.Reader, error) {
	return c.batch.reader(revspec, "blob")
}

// Tag returns a raw tag object. Caller must consume the Reader before
// making another call on C.
func (c *Batch) Tag(revspec string) (io.Reader, error) {
	return c.batch.reader(revspec, "tag")
}

// New returns a new Batch instance. It is important that ctx gets canceled
// somewhere, because if it doesn't the cat-file processes spawned by
// New() never terminate.
func New(ctx context.Context, repo *gitalypb.Repository) (*Batch, error) {
	if ctx.Done() == nil {
		panic("empty ctx.Done() in catfile.Batch.New()")
	}

	repoPath, env, err := alternates.PathAndEnv(repo)
	if err != nil {
		return nil, err
	}

	c := &Batch{}

	c.batch, err = newBatch(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	c.batchCheck, err = newBatchCheck(ctx, repoPath, env)
	if err != nil {
		return nil, err
	}

	return c, nil
}
