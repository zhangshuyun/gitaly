// +build !go1.7

package helper

import (
	"log"
	"os/exec"

	"golang.org/x/net/context"
)

// CommandWrapper handles context until we compile using Go 1.7
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
				command.Process.Kill()

			case err := <-done:
				if err != nil {
					log.Printf("process done with error = %v", err)
				} else {
					log.Print("process done gracefully without error")
				}

			}

		}()
	}

	return command
}
