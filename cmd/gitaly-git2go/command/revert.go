// +build static,system_libgit2

package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	git "github.com/libgit2/git2go/v30"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

type revertSubcommand struct {
	request string
}

func (cmd *revertSubcommand) Flags() *flag.FlagSet {
	flags := flag.NewFlagSet("revert", flag.ExitOnError)
	flags.StringVar(&cmd.request, "request", "", "git2go.RevertCommand")
	return flags
}

func (cmd *revertSubcommand) Run(context.Context, io.Reader, io.Writer) error {
	request, err := git2go.RevertCommandFromSerialized(cmd.request)
	if err != nil {
		return err
	}

	repo, err := git.OpenRepository(request.Repository)
	if err != nil {
		return fmt.Errorf("open repository: %w", err)
	}
	defer repo.Free()

	ours, err := lookupCommit(repo, request.Ours)
	if err != nil {
		return fmt.Errorf("ours commit lookup: %w", err)
	}

	revert, err := lookupCommit(repo, request.Revert)
	if err != nil {
		return fmt.Errorf("revert commit lookup: %w", err)
	}

	index, err := repo.RevertCommit(revert, ours, request.Mainline, nil)
	if err != nil {
		return fmt.Errorf("revert: %w", err)
	}
	defer index.Free()

	if index.HasConflicts() {
		return errors.New("could not revert due to conflicts")
	}

	tree, err := index.WriteTreeTo(repo)
	if err != nil {
		return fmt.Errorf("write tree: %w", err)
	}

	committer := git.Signature(git2go.NewSignature(request.AuthorName, request.AuthorMail, request.AuthorDate))
	commit, err := repo.CreateCommitFromIds("", &committer, &committer, request.Message, tree, ours.Id())
	if err != nil {
		return fmt.Errorf("create revert commit: %w", err)
	}

	response := git2go.RevertResult{
		CommitID: commit.String(),
	}

	return response.SerializeTo(os.Stdout)
}
