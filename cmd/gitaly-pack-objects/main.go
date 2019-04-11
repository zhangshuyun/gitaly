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

	if err := _main(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func _main(args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return packobjects.PackObjects(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}
