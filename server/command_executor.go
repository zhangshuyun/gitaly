package server

import (
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"gitlab.com/gitlab-org/gitaly/messaging"
)

type command struct {
	Cmd *exec.Cmd

	stdinWriter  *io.PipeWriter
	stdoutReader *io.PipeReader
	stderrReader *io.PipeReader
}

func CommandExecutor(chans *commChans) {
	rawMsg, ok := <-chans.inChan
	if !ok {
		return
	}

	msg, err := messaging.ParseMessage(rawMsg)
	if err != nil {
		return
	}
	if msg.Type != "command" {
		return
	}

	runCommand(chans, msg.GetCommand())
}

func runCommand(chans *commChans, commandMsg *messaging.Command) {
	name := commandMsg.Name
	args := commandMsg.Args

	log.Println("Executing command:", name, "with args", args)

	cmd := newCommand(name, args...)

	// Start the command in its own process group (nice for signalling)
	cmd.Cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cmd.Env = commandMsg.Environ

	state, err := cmd.Run(chans)
	if err != nil {
		return
	}

	if !state.Success() {
		exitStatus := int32(extractExitStatusFromProcessState(state))
		chans.outChan <- messaging.NewExitMessage(exitStatus)
		return
	}

	chans.outChan <- messaging.NewExitMessage(0)
}

func extractExitStatusFromProcessState(state *os.ProcessState) int {
	status := state.Sys().(syscall.WaitStatus)

	if status.Exited() {
		return status.ExitStatus()
	}

	return 255
}

func streamOut(streamName string, streamPipe io.Reader, chans *commChans, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	// TODO: Move buffer out of the loop and use defer instead of finished
	finished := false

	for {
		buffer := make([]byte, bytes.MinRead)

		n, err := streamPipe.Read(buffer)
		if err == io.EOF {
			finished = true
		}

		if n < bytes.MinRead {
			buffer = buffer[:n]
		}

		chans.outChan <- messaging.NewOutputMessage(streamName, buffer)

		if finished {
			return
		}
	}
}

func streamIn(streamPipe *io.PipeWriter, chans *commChans) {
	defer streamPipe.Close()

	for {
		rawMsg, ok := <-chans.inChan
		if !ok {
			return
		}

		msg, err := messaging.ParseMessage(rawMsg)
		if msg.Type != "stdin" {
			continue
		}

		stdin := msg.GetInput().Stdin
		if len(stdin) == 0 {
			return
		}

		_, err = streamPipe.Write(stdin)
		if err != nil {
			return
		}
	}
}

func newCommand(name string, args ...string) *command {
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()

	cmd := exec.Command(name, args...)

	cmd.Stdin = stdinReader
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	return &command{
		Cmd:          cmd,
		stdinWriter:  stdinWriter,
		stdoutReader: stdoutReader,
		stderrReader: stderrReader,
	}
}

func (cmd *command) Run(chans *commChans) (*os.ProcessState, error) {
	waitGrp := &sync.WaitGroup{}

	go streamOut("stdout", cmd.stdoutReader, chans, waitGrp)
	go streamOut("stderr", cmd.stderrReader, chans, waitGrp)
	waitGrp.Add(2)

	go streamIn(cmd.stdinWriter, chans)

	if err := cmd.Cmd.Start(); err != nil {
		return nil, err
	}
	state, err := cmd.Cmd.Process.Wait()
	if err != nil {
		return nil, err
	}

	cmd.Cmd.Stdout.(*io.PipeWriter).Close()
	cmd.Cmd.Stderr.(*io.PipeWriter).Close()
	waitGrp.Wait()

	return state, nil
}
