package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"gitlab.com/gitlab-org/labkit/correlation"
)

type Trace2Event struct {
	Event   string    `json:"event"`
	Sid     string    `json:"sid"`
	Thread  string    `json:"thread"`
	Time    time.Time `json:"time"`
	File    string    `json:"file"`
	Line    int       `json:"line"`
	AbsTime float64   `json:"t_abs"`
	Code    int       `json:"code"`
}

func (c *Command) enableTrace2(ctx context.Context) (error, func()) {
	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("Trace2 enable: %w", err), nil
	}

	// Git will block unless someone reads from the pipe
	// so start a goroutine to continiuosly reads.
	go func() {
		dec := json.NewDecoder(r)
		var ev Trace2Event

		for {
			err := dec.Decode(&ev)
			if err == nil {
				c.span.LogKV(
					"trace2.event", ev.Event,
					"trace2.sid", ev.Sid,
					"trace2.thread", ev.Thread,
					"trace2.time", ev.Time,
					"trace2.file", ev.File,
					"trace2.line", ev.Line,
					"trace2.t_abs", ev.AbsTime,
					"trace2.code", ev.Code)
			} else if errors.Is(err, os.ErrClosed) {
				break
			}
			// keep reading as long the pipe is open
		}
	}()

	c.cmd.ExtraFiles = append(c.cmd.ExtraFiles, w)
	c.cmd.Env = append(c.cmd.Env,
		fmt.Sprintf("GIT_TRACE2_EVENT=%d", 2+len(c.cmd.ExtraFiles)), // add 2 to account for stdin (0), stdout (1) & stderr (2)
		fmt.Sprintf("GIT_TRACE2_PARENT_SID=%s", correlation.ExtractFromContextOrGenerate(ctx)))

	return nil, func() {
		r.Close()
		w.Close()
	}
}
