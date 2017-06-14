package main

import (
	"build"
	"fmt"
	"os"
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

	return os.RemoveAll(config.BuildDir)
}
