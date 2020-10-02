// +build static,system_libgit2

package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	git "github.com/libgit2/git2go/v30"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git2go"
)

type mergeSubcommand struct {
	request string
}

func (cmd *mergeSubcommand) Flags() *flag.FlagSet {
	flags := flag.NewFlagSet("merge", flag.ExitOnError)
	flags.StringVar(&cmd.request, "request", "", "git2go.MergeCommand")
	return flags
}

func lookupCommit(repo *git.Repository, ref string) (*git.Commit, error) {
	object, err := repo.RevparseSingle(ref)
	if err != nil {
		return nil, fmt.Errorf("could not lookup reference: %w", err)
	}

	peeled, err := object.Peel(git.ObjectCommit)
	if err != nil {
		return nil, fmt.Errorf("could not peel reference: %w", err)
	}

	commit, err := peeled.AsCommit()
	if err != nil {
		return nil, fmt.Errorf("could not cast to commit: %w", err)
	}

	return commit, nil
}

func sanitizeSignatureInfo(info string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '<', '>', '\n':
			return -1
		default:
			return r
		}
	}, info)
}

func (cmd *mergeSubcommand) Run() error {
	request, err := git2go.MergeCommandFromSerialized(cmd.request)
	if err != nil {
		return err
	}

	if request.AuthorDate.IsZero() {
		request.AuthorDate = time.Now()
	}

	repo, err := git.OpenRepository(request.Repository)
	if err != nil {
		return fmt.Errorf("could not open repository: %w", err)
	}

	ours, err := lookupCommit(repo, request.Ours)
	if err != nil {
		return fmt.Errorf("could not lookup commit %q: %w", request.Ours, err)
	}

	theirs, err := lookupCommit(repo, request.Theirs)
	if err != nil {
		return fmt.Errorf("could not lookup commit %q: %w", request.Theirs, err)
	}

	index, err := repo.MergeCommits(ours, theirs, nil)
	if err != nil {
		return fmt.Errorf("could not merge commits: %w", err)
	}

	if index.HasConflicts() {
		return errors.New("could not auto-merge due to conflicts")
	}

	tree, err := index.WriteTreeTo(repo)
	if err != nil {
		return fmt.Errorf("could not write tree: %w", err)
	}

	committer := git.Signature{
		Name:  sanitizeSignatureInfo(request.AuthorName),
		Email: sanitizeSignatureInfo(request.AuthorMail),
		When:  request.AuthorDate,
	}

	commit, err := repo.CreateCommitFromIds("", &committer, &committer, request.Message, tree, ours.Id(), theirs.Id())
	if err != nil {
		return fmt.Errorf("could not create merge commit: %w", err)
	}

	response := git2go.MergeResult{
		CommitID: commit.String(),
	}

	serialized, err := response.Serialize()
	if err != nil {
		return err
	}

	fmt.Println(serialized)

	return nil
}
