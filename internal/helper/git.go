package helper

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

// GitCommandHandler is the signature for git command handlers
type GitCommandHandler func(cmd *exec.Cmd, stdout io.Reader) error

// GitCommand uses the given handler to process the stdout of a git command
func GitCommand(handler GitCommandHandler, glID string, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Explicitly set the environment for the Git command
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", os.Getenv("HOME")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("LD_LIBRARY_PATH=%s", os.Getenv("LD_LIBRARY_PATH")),
		fmt.Sprintf("GL_ID=%s", glID),
		fmt.Sprintf("GL_PROTOCOL=http"),
	}

	// If we don't do something with cmd.Stderr, Git errors will be lost
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("GitCommand: stdout: %v", err)
	}
	defer stdout.Close()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("GitCommand: start %v: %v", cmd.Args, err)
	}
	defer CleanUpProcessGroup(cmd) // Ensure brute force subprocess clean-up

	if err := handler(cmd, stdout); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("GitCommand: wait for %v: %v", cmd.Args, err)
	}

	return nil
}
