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
		go func() {
			<-ctx.Done()
			log.Printf("Context done, killing process")
			command.Kill()
		}()
	}

	return command
}
