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
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/labkit/correlation"
)

// batchCheck encapsulates a 'git cat-file --batch-check' process
type batchCheck struct {
	r *bufio.Reader
	w io.WriteCloser
	sync.Mutex
}

func newBatchCheck(ctx context.Context, locator storage.Locator, repo repository.GitRepo) (*batchCheck, error) {
	bc := &batchCheck{}

	var stdinReader io.Reader
	stdinReader, bc.w = io.Pipe()

	// batch processes are long-lived and reused across RPCs,
	// so we de-correlate the process from the RPC
	ctx = correlation.ContextWithCorrelation(ctx, "")
	ctx = opentracing.ContextWithSpan(ctx, nil)

	batchCmd, err := git.NewCommand(ctx, repo, nil,
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

func (bc *batchCheck) info(spec string) (*ObjectInfo, error) {
	bc.Lock()
	defer bc.Unlock()

	if _, err := fmt.Fprintln(bc.w, spec); err != nil {
		return nil, err
	}

	return ParseObjectInfo(bc.r)
}
