// +build static,system_libgit2

package git2go_test

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/command"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

func init() {
	// When the test binary is compiled with libgit2, we'll run
	// the tests additionally by making direct calls to gitaly-git2go
	// without using a subprocess. This makes it possible to gather
	// test coverage for the code in the subprocess as well.
	bridge := git2go.NewWithRunner(func(ctx context.Context, binaryPath string, stdin io.Reader, args ...string) (*bytes.Buffer, error) {
		if stdin == nil {
			stdin = bytes.NewReader(nil)
		}

		stderr := &bytes.Buffer{}
		stdout := &bytes.Buffer{}
		errcode := command.Main(command.Process{
			Args:   append([]string{"in-process"}, args...),
			Stdin:  stdin,
			Stdout: stdout,
			Stderr: stderr,
		})

		if errcode != 0 {
			return nil, fmt.Errorf("code: %d, stderr: %q", errcode, stderr)
		}

		return stdout, nil
	})

	executors["in-process"] = bridge
}
