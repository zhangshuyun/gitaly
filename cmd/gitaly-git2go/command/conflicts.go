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
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type conflictsSubcommand struct {
	request string
}

func (cmd *conflictsSubcommand) Flags() *flag.FlagSet {
	flags := flag.NewFlagSet("conflicts", flag.ExitOnError)
	flags.StringVar(&cmd.request, "request", "", "git2go.ConflictsCommand")
	return flags
}

func conflictEntryFromIndex(entry *git.IndexEntry) git2go.ConflictEntry {
	if entry == nil {
		return git2go.ConflictEntry{}
	}
	return git2go.ConflictEntry{
		Path: entry.Path,
		Mode: int32(entry.Mode),
	}
}

func conflictContent(repo *git.Repository, conflict git.IndexConflict) ([]byte, error) {
	var ancestor, our, their git.MergeFileInput

	for entry, input := range map[*git.IndexEntry]*git.MergeFileInput{
		conflict.Ancestor: &ancestor,
		conflict.Our:      &our,
		conflict.Their:    &their,
	} {
		if entry == nil {
			continue
		}

		blob, err := repo.LookupBlob(entry.Id)
		if err != nil {
			return nil, helper.ErrPreconditionFailedf("could not get conflicting blob: %w", err)
		}

		input.Path = entry.Path
		input.Mode = uint(entry.Mode)
		input.Contents = blob.Contents()
	}

	merge, err := git.MergeFile(ancestor, our, their, nil)
	if err != nil {
		return nil, fmt.Errorf("could not compute conflicts: %w", err)
	}

	return merge.Contents, nil
}

func conflictError(code codes.Code, message string) error {
	result := git2go.ConflictsResult{
		Error: git2go.ConflictError{
			Code:    code,
			Message: message,
		},
	}

	if err := result.SerializeTo(os.Stdout); err != nil {
		return err
	}

	return nil
}

// Run performs a merge and prints resulting conflicts to stdout.
func (cmd *conflictsSubcommand) Run(context.Context, io.Reader, io.Writer) error {
	request, err := git2go.ConflictsCommandFromSerialized(cmd.request)
	if err != nil {
		return err
	}

	repo, err := git.OpenRepository(request.Repository)
	if err != nil {
		return fmt.Errorf("could not open repository: %w", err)
	}

	oursOid, err := git.NewOid(request.Ours)
	if err != nil {
		return err
	}

	ours, err := repo.LookupCommit(oursOid)
	if err != nil {
		return err
	}

	theirsOid, err := git.NewOid(request.Theirs)
	if err != nil {
		return err
	}

	theirs, err := repo.LookupCommit(theirsOid)
	if err != nil {
		return err
	}

	index, err := repo.MergeCommits(ours, theirs, nil)
	if err != nil {
		return conflictError(codes.FailedPrecondition, fmt.Sprintf("could not merge commits: %v", err))
	}

	conflicts, err := index.ConflictIterator()
	if err != nil {
		return fmt.Errorf("could not get conflicts: %w", err)
	}

	var result git2go.ConflictsResult
	for {
		conflict, err := conflicts.Next()
		if err != nil {
			var gitError git.GitError
			if errors.As(err, &gitError) && gitError.Code != git.ErrIterOver {
				return err
			}
			break
		}

		content, err := conflictContent(repo, conflict)
		if err != nil {
			if status, ok := status.FromError(err); ok {
				return conflictError(status.Code(), status.Message())
			}
			return err
		}

		result.Conflicts = append(result.Conflicts, git2go.Conflict{
			Ancestor: conflictEntryFromIndex(conflict.Ancestor),
			Our:      conflictEntryFromIndex(conflict.Our),
			Their:    conflictEntryFromIndex(conflict.Their),
			Content:  content,
		})
	}

	if err := result.SerializeTo(os.Stdout); err != nil {
		return err
	}

	return nil
}
