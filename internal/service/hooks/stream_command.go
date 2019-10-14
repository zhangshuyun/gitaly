package hook

import (
	"context"
	"io"
	"os/exec"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
)

// streamResponseGenerator is a function that generates responses for sending to a stream
type streamResponseGenerator func(p []byte) interface{}

type streamLastResponseGenerator func(success bool) interface{}

func streamCommandResponse(
	ctx context.Context,
	stream grpc.ServerStream,
	c *exec.Cmd,
	stdin io.Reader,
	stdoutGen streamResponseGenerator,
	stderrGen streamResponseGenerator,
	lastRespGen streamLastResponseGenerator,
	env []string,
) error {
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.SendMsg(stdoutGen(p))
	})
	stderr := streamio.NewWriter(func(p []byte) error {
		return stream.SendMsg(stderrGen(p))
	})
	cmd, err := command.New(ctx, c, stdin, stdout, stderr, env...)
	if err != nil {
		return helper.ErrInternal(err)
	}

	success := true

	// handle an error from the ruby hook by setting success = false
	if err = cmd.Wait(); err != nil {
		grpc_logrus.Extract(ctx).WithError(err).Error("failed to run git hook")
		success = false
	}

	if err := stream.SendMsg(lastRespGen(success)); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
