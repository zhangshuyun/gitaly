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
	config, err := build.ReadConfig()
	if err != nil {
		return err
	}
	govendor, err := exec.LookPath("govendor")
	if err != nil {
		return err
	}
	os.Setenv("GOPATH", config.BuildGopath)
	if err := os.Chdir(config.PackageBuildDir()); err != nil {
		return err
	}
	return syscall.Exec(govendor, []string{"govendor", "status"}, os.Environ())
}
