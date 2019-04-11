package main

import (
	"context"
	"log"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/packobjects"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("not enough argument to pack-objects hook")
	}

	if err := _main(); err != nil {
		log.Fatal(err)
	}
}

func _main() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	return packobjects.PackObjects(ctx, wd, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}
