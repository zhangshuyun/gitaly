package git2go

import (
	"bytes"
	"context"
	"io"
)

// RunFunc runns a command. Used for testing.
type RunFunc func(context.Context, string, io.Reader, ...string) (*bytes.Buffer, error)

// Executor executes gitaly-git2go.
type Executor struct {
	binaryPath string
	run        RunFunc
}

// New returns a new gitaly-git2go executor using the provided binary.
func New(binaryPath string) Executor {
	return Executor{
		binaryPath: binaryPath,
		run:        run,
	}
}

// NewWithRunner returns an Executor using the given run method. Used for testing.
func NewWithRunner(run RunFunc) Executor {
	return Executor{run: run}
}
