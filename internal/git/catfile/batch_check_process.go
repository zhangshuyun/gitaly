package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// batchCheckProcess encapsulates a 'git cat-file --batch-check' process
type batchCheckProcess struct {
	r *bufio.Reader
	w io.WriteCloser
	sync.Mutex
}

func newBatchCheckProcess(ctx context.Context, gitCmdFactory git.CommandFactory, repo repository.GitRepo) (*batchCheckProcess, error) {
	bc := &batchCheckProcess{}

	var stdinReader io.Reader
	stdinReader, bc.w = io.Pipe()

	// batch processes are long-lived and reused across RPCs,
	// so we de-correlate the process from the RPC
	ctx = correlation.ContextWithCorrelation(ctx, "")
	ctx = opentracing.ContextWithSpan(ctx, nil)

	batchCmd, err := gitCmdFactory.New(ctx, repo, nil,
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

	bc.r = bufio.NewReader(batchCmd)
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		bc.w.Close()
	}()

	if injectSpawnErrors {
		// Testing only: intentionally leak process
		return nil, &simulatedBatchSpawnError{}
	}

	return bc, nil
}

func (bc *batchCheckProcess) info(revision git.Revision) (*ObjectInfo, error) {
	bc.Lock()
	defer bc.Unlock()

	if _, err := fmt.Fprintln(bc.w, revision.String()); err != nil {
		return nil, err
	}

	return ParseObjectInfo(bc.r)
}
