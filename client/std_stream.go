package client

import (
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// StdoutStderrResponse is an interface for RPC responses that need to stream stderr and stdout
type StdoutStderrResponse interface {
	GetExitStatus() *gitalypb.ExitStatus
	GetStderr() []byte
	GetStdout() []byte
}

// Sender is a function that sends input data to the stream
type Sender func(chan error)

// StreamHandler takes care of sending and receiving to and from the stream
func StreamHandler(recv func() (StdoutStderrResponse, error), send Sender, stdout, stderr io.Writer) (int32, error) {
	var (
		exitStatus int32
		err        error
		resp       StdoutStderrResponse
	)

	errC := make(chan error, 1)

	go send(errC)

	for {
		resp, err = recv()
		if err != nil {
			break
		}
		if resp.GetExitStatus() != nil {
			exitStatus = resp.GetExitStatus().GetValue()
		}

		if len(resp.GetStderr()) > 0 {
			if _, err = stderr.Write(resp.GetStderr()); err != nil {
				break
			}
		}

		if len(resp.GetStdout()) > 0 {
			if _, err = stdout.Write(resp.GetStdout()); err != nil {
				break
			}
		}
	}
	if err == io.EOF {
		err = nil
	}

	if err != nil {
		return exitStatus, err
	}

	select {
	case errSend := <-errC:
		if errSend != nil {
			// This should not happen
			errSend = fmt.Errorf("stdin send error: %v", errSend)
		}
		return exitStatus, errSend
	default:
		return exitStatus, nil
	}
}
