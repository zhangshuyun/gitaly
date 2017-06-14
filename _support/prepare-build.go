package main

import (
	"build"
	"context"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := build.ReadConfig()
	if err != nil {
		return err
	}

	if err := os.RemoveAll(config.BuildGopath); err != nil {
		return err
	}
	if err := os.MkdirAll(config.PackageBuildDir(), 0755); err != nil {
		return err
	}

	tarSource := exec.CommandContext(ctx, "tar", "-cf", "-", "--exclude", config.BuildGopath, "--exclude", ".git", ".")
	tarSource.Stderr = os.Stderr
	sourcePipe, err := tarSource.StdoutPipe()
	if err != nil {
		return err
	}

	tarDest := exec.CommandContext(ctx, "tar", "-xf", "-")
	tarDest.Stdin = sourcePipe
	tarDest.Stderr = os.Stderr
	tarDest.Dir = config.PackageBuildDir()

	for _, err := range []error{tarSource.Start(), tarDest.Start()} {
		if err != nil {
			return err
		}
	}

	if err := sourcePipe.Close(); err != nil {
		return err
	}

	for _, err := range []error{tarSource.Wait(), tarDest.Wait()} {
		if err != nil {
			return err
		}
	}

	return nil
}
