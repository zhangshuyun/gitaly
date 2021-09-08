package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// objectInfoReader is a reader for Git object information. This reader is implemented via a
// long-lived  `git cat-file --batch-check` process such that we do not have to spawn a separate
// process per object info we're about to read.
type objectInfoReader struct {
	r *bufio.Reader
	w io.WriteCloser
	sync.Mutex
}

func (bc *BatchCache) newObjectInfoReader(ctx context.Context, repo git.RepositoryExecutor) (*objectInfoReader, error) {
	objectInfoReader := &objectInfoReader{}

	var stdinReader io.Reader
	stdinReader, objectInfoReader.w = io.Pipe()

	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.ObjectInfoReader")

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

	objectInfoReader.r = bufio.NewReader(batchCmd)
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		objectInfoReader.w.Close()
		span.Finish()
	}()

	return objectInfoReader, nil
}

func (o *objectInfoReader) info(revision git.Revision) (*ObjectInfo, error) {
	o.Lock()
	defer o.Unlock()

	if _, err := fmt.Fprintln(o.w, revision.String()); err != nil {
		return nil, err
	}

	return ParseObjectInfo(o.r)
}
