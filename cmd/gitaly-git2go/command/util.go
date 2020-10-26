// +build static,system_libgit2

package command

import (
	"fmt"

	git "github.com/libgit2/git2go/v30"
)

func lookupCommit(repo *git.Repository, ref string) (*git.Commit, error) {
	object, err := repo.RevparseSingle(ref)
	if err != nil {
		return nil, fmt.Errorf("could not lookup reference %q: %w", ref, err)
	}

	peeled, err := object.Peel(git.ObjectCommit)
	if err != nil {
		return nil, fmt.Errorf("could not peel reference %q: %w", ref, err)
	}

	commit, err := peeled.AsCommit()
	if err != nil {
		return nil, fmt.Errorf("reference %q is not a commit: %w", ref, err)
	}

	return commit, nil
}
