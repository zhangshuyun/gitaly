// +build static,system_libgit2

package main

import (
	"fmt"

	git "github.com/libgit2/git2go/v31"
)

func lookupCommit(repo *git.Repository, ref string) (*git.Commit, error) {
	object, err := repo.RevparseSingle(ref)
	if err != nil {
		return nil, fmt.Errorf("lookup commit %q: %w", ref, err)
	}

	peeled, err := object.Peel(git.ObjectCommit)
	if err != nil {
		return nil, fmt.Errorf("lookup commit %q: peel: %w", ref, err)
	}

	commit, err := peeled.AsCommit()
	if err != nil {
		return nil, fmt.Errorf("lookup commit %q: as commit: %w", ref, err)
	}

	return commit, nil
}
