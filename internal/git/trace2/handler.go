// trace2 provides utilities to leverage Gits trace2 functionalities within
// Gitaly.
package trace2

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	// By setting this environment variable for Git process, it will enable
	// the trace 2. Furhter, the value it will be set to tells Git where
	// to write the output to. Gitaly uses an open file descriptor.
	trace2EventFDKey = "GIT_TRACE2_EVENT"

	// Git continiously shells out to child processes and will set the SID
	// as environment variable. The child than appends their own unique ID.
	// For Gitaly we set the SID to the correlation ID to allow for
	// correlation throughout GitLabs components.
	sessionIDKey = "GIT_TRACE2_PARENT_SID"
)

// CopyHandler takes the trace2 stream, and copies it over to a passed io.Writer
// Note it still receives the stream on a fresh file descriptor, not 2.
// Returns environment variables which MUST be injected by the
// caller into the spawned Git process; else the sink will never be
// closed and a resource leak occurs.
func CopyHandler(ctx context.Context, cmd *exec.Cmd, sink io.Writer) ([]string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	cancelCtx, _ := context.WithCancel(ctx)

	go func() {
		for {
			select {
			case <-cancelCtx.Done():
				w.Close()
				r.Close()
				break
			default:
				io.Copy(sink, r)
			}

		}
	}()

	cmd.ExtraFiles = append(cmd.ExtraFiles, w)

	return []string{
		// Plus 2 accounts for stdin (0), stdout (1), and stderr (2).
		fmt.Sprintf("%s=%d", trace2EventFDKey, 2+len(cmd.ExtraFiles)),
		fmt.Sprintf("%s=%s", sessionIDKey, correlation.ExtractFromContextOrGenerate(ctx)),
	}, nil
}
