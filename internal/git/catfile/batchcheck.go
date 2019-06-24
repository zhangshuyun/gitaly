package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/git"
)

// batchCheck encapsulates a 'git cat-file --batch-check' process
type batchCheck struct {
	r     *bufio.Reader
	w     io.WriteCloser
	cache map[string]ObjectInfo
	sync.Mutex
}

func newBatchCheck(ctx context.Context, repoPath string, env []string) (*batchCheck, error) {
	bc := &batchCheck{cache: make(map[string]ObjectInfo)}

	var stdinReader io.Reader
	stdinReader, bc.w = io.Pipe()
	batchCmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch-check"}
	batchCmd, err := git.BareCommand(ctx, stdinReader, nil, nil, env, batchCmdArgs...)
	if err != nil {
		return nil, err
	}

	bc.r = bufio.NewReader(batchCmd)
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		bc.w.Close()
	}()

	return bc, nil
}

func (bc *batchCheck) info(spec string) (*ObjectInfo, error) {
	if oi, ok := bc.cachedInfo(spec); ok {
		return oi, nil
	}

	bc.Lock()
	defer bc.Unlock()

	if _, err := fmt.Fprintln(bc.w, spec); err != nil {
		return nil, err
	}

	oi, err := parseObjectInfo(bc.r)
	if err != nil {
		return nil, err
	}

	bc.cache[spec] = *oi

	return oi, nil
}

func (bc *batchCheck) cachedInfo(spec string) (*ObjectInfo, bool) {
	bc.Lock()
	defer bc.Unlock()

	if oi, ok := bc.cache[spec]; ok {
		return &oi, true
	}

	return nil, false
}
