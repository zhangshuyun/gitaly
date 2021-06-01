package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// batchCheckProcess encapsulates a 'git cat-file --batch-check' process
type batchCheckProcess struct {
	r *bufio.Reader
	w io.WriteCloser
	sync.Mutex
}

func (bc *BatchCache) newBatchCheckProcess(ctx context.Context, repo git.RepositoryExecutor) (*batchCheckProcess, error) {
	process := &batchCheckProcess{}

	var stdinReader io.Reader
	stdinReader, process.w = io.Pipe()

	// batch processes are long-lived and reused across RPCs,
	// so we de-correlate the process from the RPC
	ctx = correlation.ContextWithCorrelation(ctx, "")
	ctx = opentracing.ContextWithSpan(ctx, nil)

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

	process.r = bufio.NewReader(batchCmd)
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		process.w.Close()
	}()

	if bc.injectSpawnErrors {
		// Testing only: intentionally leak process
		return nil, &simulatedBatchSpawnError{}
	}

	return process, nil
}

func (bc *batchCheckProcess) info(revision git.Revision) (*ObjectInfo, error) {
	bc.Lock()
	defer bc.Unlock()

	if _, err := fmt.Fprintln(bc.w, revision.String()); err != nil {
		return nil, err
	}

	return ParseObjectInfo(bc.r)
}
