package helper

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/net/context"
)

// Command encapsulates operations with commands creates with NewCommand
type Command struct {
	io.Reader
	*exec.Cmd
}

// Kill cleans the subprocess group of the command. Callers should defer a call
// to kill after they get the command from NewCommand
func (c *Command) Kill() {
	CleanUpProcessGroup(c.Cmd)
}

// GitCommandReader creates a git Command with the given args
func GitCommandReader(ctx context.Context, args ...string) (*Command, error) {
	// TODO: when we switch to Go 1.7, switch to using
	// exec.CommandContext
	return NewCommand(CommandWrapper(ctx, "git", args...), nil, nil)
}

// NewCommand creates a Command from an exec.Cmd
func NewCommand(cmd *exec.Cmd, stdin io.Reader, stdout io.Writer, env ...string) (*Command, error) {
	command := &Command{Cmd: cmd}

	// Explicitly set the environment for the command
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", os.Getenv("HOME")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("LD_LIBRARY_PATH=%s", os.Getenv("LD_LIBRARY_PATH")),
	}
	cmd.Env = append(cmd.Env, env...)

	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if stdin != nil {
		cmd.Stdin = stdin
	}

	if stdout != nil {
		// We don't assign a reader if an stdout override was passed. We assume
		// output is going to be directly handled by the caller.
		cmd.Stdout = stdout
	} else {
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("GitCommand: stdout: %v", err)
		}
		command.Reader = pipe
	}

	// If we don't do something with cmd.Stderr, Git errors will be lost
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("GitCommand: start %v: %v", cmd.Args, err)
	}

	return command, nil
}

func cleanUpProcessGroupNoWait(cmd *exec.Cmd) {
	process := cmd.Process
	if process != nil && process.Pid > 0 {
		// Send SIGTERM to the process group of cmd
		syscall.Kill(-process.Pid, syscall.SIGTERM)
	}

}

// CleanUpProcessGroup will send a SIGTERM signal to the process group
// belonging to the `cmd` process
func CleanUpProcessGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}

	cleanUpProcessGroupNoWait(cmd)

	// reap our child process
	cmd.Wait()
}

// ExitStatus will return the exit-code from an error
func ExitStatus(err error) (int, bool) {
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		return 0, false
	}

	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok {
		return 0, false
	}

	return waitStatus.ExitStatus(), true
}

// CommandWrapper ensures that the command is executed within a context,
// and ensures that the process group is terminated with the
func CommandWrapper(ctx context.Context, name string, arg ...string) *exec.Cmd {
	command := exec.Command(name, arg...)

	if ctx != nil {
		// Create a channel to listen to the command completion
		done := make(chan error, 1)
		go func() {
			done <- command.Wait()
		}()

		// Wait for the process to shutdown or the
		// context to be complete
		go func() {
			select {
			case <-ctx.Done():
				log.Printf("Context done, killing process")
				cleanUpProcessGroupNoWait(command)

			case err := <-done:
				if err != nil {
					log.Printf("process done with error = %v", err)
				} else {
					log.Print("process done gracefully without error")
				}
				cleanUpProcessGroupNoWait(command)

			}

		}()
	}

	return command
}
