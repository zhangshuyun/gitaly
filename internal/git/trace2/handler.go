// trace2 provides utilities to leverage Gits trace2 functionalities within
// Gitaly.
package trace2

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/log"
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

// LogHandler takes the trace2 events and logs child processes starting as well
// as their completion.
// Note the exec.Cmd is altered to include a FD for git to write the output to.
func LogHandler(ctx context.Context, cmd *exec.Cmd) ([]string, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	go func() {
		cancelCtx, _ := context.WithCancel(ctx)
		bufReader := bufio.NewReader(r)

		for {
			select {
			case <-cancelCtx.Done():
				w.Close()
				r.Close()
				break
			default:
				err := logTraceEvents(cancelCtx, bufReader)
				if err == io.EOF {
					continue
				}
				if err != nil {
					log.WithError(err).Error("failed to parse trace2 event")
				}
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

type trace2Event struct {
	Event    string   `json:"event"`
	Argv     []string `json:"argv"` // Only present at child_start events
	SID      string   `json:"sid"`
	ExitCode int      `json:"code"`
}

const (
	childEventStart = "child_start"
	childEventExit  = "child_exit"
)

func shouldLogEvent(e *trace2Event) bool {
	return e.Event == childEventStart || e.Event == childEventExit
}

func logTraceEvents(ctx context.Context, r *bufio.Reader) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return err
	}

	event := &trace2Event{}
	if err := json.Unmarshal(line, event); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Warn("error decoding trace2 json")

		// This is not an error the caller can handle
		return nil
	}

	if shouldLogEvent(event) {
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"event":     event.Event,
			"SID":       event.SID,
			"argv":      event.Argv,
			"exit_code": event.ExitCode,
		}).Info("trace2 event")
	}

	return nil
}
