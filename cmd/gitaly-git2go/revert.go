// +build static,system_libgit2

package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

type revertSubcommand struct{}

func (cmd *revertSubcommand) Flags() *flag.FlagSet {
	return flag.NewFlagSet("revert", flag.ExitOnError)
}

func (cmd *revertSubcommand) Run(ctx context.Context, r io.Reader, w io.Writer) error {
	var request git2go.RevertCommand
	if err := gob.NewDecoder(r).Decode(&request); err != nil {
		return err
	}

	commitID, err := cmd.revert(ctx, &request)
	return gob.NewEncoder(w).Encode(git2go.Result{
		CommitID: commitID,
		Error:    git2go.SerializableError(err),
	})
}

func (cmd *revertSubcommand) verify(ctx context.Context, r *git2go.RevertCommand) error {
	if r.Repository == "" {
		return errors.New("missing repository")
	}
	if r.AuthorName == "" {
		return errors.New("missing author name")
	}
	if r.AuthorMail == "" {
		return errors.New("missing author mail")
	}
	if r.Message == "" {
		return errors.New("missing message")
	}
	if r.Ours == "" {
		return errors.New("missing ours")
	}
	if r.Revert == "" {
		return errors.New("missing revert")
	}
	return nil
}

func (cmd *revertSubcommand) revert(ctx context.Context, request *git2go.RevertCommand) (string, error) {
	if err := cmd.verify(ctx, request); err != nil {
		return "", err
	}
	repo, err := git.OpenRepository(request.Repository)
	if err != nil {
		return "", fmt.Errorf("open repository: %w", err)
	}
	defer repo.Free()

	ours, err := lookupCommit(repo, request.Ours)
	if err != nil {
		return "", fmt.Errorf("ours commit lookup: %w", err)
	}

	revert, err := lookupCommit(repo, request.Revert)
	if err != nil {
		return "", fmt.Errorf("revert commit lookup: %w", err)
	}

	index, err := repo.RevertCommit(revert, ours, request.Mainline, nil)
	if err != nil {
		return "", fmt.Errorf("revert: %w", err)
	}
	defer index.Free()

	if index.HasConflicts() {
		return "", git2go.HasConflictsError{}
	}

	tree, err := index.WriteTreeTo(repo)
	if err != nil {
		return "", fmt.Errorf("write tree: %w", err)
	}

	committer := git.Signature(git2go.NewSignature(request.AuthorName, request.AuthorMail, request.AuthorDate))
	commit, err := repo.CreateCommitFromIds("", &committer, &committer, request.Message, tree, ours.Id())
	if err != nil {
		return "", fmt.Errorf("create revert commit: %w", err)
	}

	return commit.String(), nil
}
