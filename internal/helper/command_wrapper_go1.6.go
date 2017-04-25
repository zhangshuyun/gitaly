// +build !go1.7

package helper

import (
	"os/exec"

	"golang.org/x/net/context"
)

// CommandWrapper handles context until we compile using Go 1.7
func CommandWrapper(ctx context.Context, name string, arg ...string) *exec.Cmd {
	return exec.Command(name, arg...)
}
