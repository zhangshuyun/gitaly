package main

import (
	"build"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
if len(os.Args) <= 1 {
return fmt.Errorf("need at least one argument")
}
	config, err := build.ReadConfig()
	if err != nil {
		return err
	}

	executable, err := exec.LookPath(os.Args[1])
	if err != nil {
		return err
	}
	os.Setenv("GOPATH", config.BuildGopath)
	if err := os.Chdir(config.PackageBuildDir()); err != nil {
		return err
	}
	return syscall.Exec(executable, os.Args[1:], os.Environ())
}
