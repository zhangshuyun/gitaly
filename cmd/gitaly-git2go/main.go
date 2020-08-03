package main

import (
	"os"

	git "github.com/libgit2/git2go/v30"
)

func main() {
	repo, err := git.OpenRepository(".")
	if err != nil {
		os.Exit(1)
	}
	defer repo.Free()

	head, err := repo.Head()
	if err != nil {
		os.Exit(1)
	}
	defer head.Free()

	println(head.Target().String())
}
