// +build static,system_libgit2
package main

import (
	"os"

	"gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/command"
)

func main() {
	os.Exit(command.Main(command.Process{
		Args:   os.Args,
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}
