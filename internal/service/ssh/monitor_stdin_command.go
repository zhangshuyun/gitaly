package ssh

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
)

func monitorStdinCommand(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, env []string, globals []git.Option, sc git.SubCmd) (*command.Command, *pktline.ReadMonitor, error) {
	stdinPipe, monitor, err := pktline.NewReadMonitor(ctx, stdin)
	if err != nil {
		return nil, nil, fmt.Errorf("create monitor: %v", err)
	}

	cmd, err := git.SafeBareCmd(ctx, stdinPipe, stdout, stderr, env, globals, sc)
	stdinPipe.Close() // this now belongs to cmd
	if err != nil {
		return nil, nil, fmt.Errorf("start cmd: %v", err)
	}

	return cmd, monitor, err
}
