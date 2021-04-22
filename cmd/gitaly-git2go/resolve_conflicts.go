// +build static,system_libgit2

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

type resolveSubcommand struct {
}

func (cmd *resolveSubcommand) Flags() *flag.FlagSet {
	return flag.NewFlagSet("resolve", flag.ExitOnError)
}

func (cmd resolveSubcommand) Run(_ context.Context, r io.Reader, w io.Writer) error {
	var request git2go.ResolveCommand
	if err := gob.NewDecoder(r).Decode(&request); err != nil {
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
		return fmt.Errorf("ours commit lookup: %w", err)
	}

	theirs, err := lookupCommit(repo, request.Theirs)
	if err != nil {
		return fmt.Errorf("theirs commit lookup: %w", err)
	}

	index, err := repo.MergeCommits(ours, theirs, nil)
	if err != nil {
		return fmt.Errorf("could not merge commits: %w", err)
	}

	ci, err := index.ConflictIterator()
	if err != nil {
		return err
	}

	type paths struct {
		theirs, ours string
	}
	conflicts := map[paths]git.IndexConflict{}

	for {
		c, err := ci.Next()
		if git.IsErrorCode(err, git.ErrIterOver) {
			break
		}
		if err != nil {
			return err
		}

		if c.Our.Path == "" || c.Their.Path == "" {
			return errors.New("conflict side missing")
		}

		k := paths{
			theirs: c.Their.Path,
			ours:   c.Our.Path,
		}
		conflicts[k] = c
	}

	odb, err := repo.Odb()
	if err != nil {
		return err
	}

	for _, r := range request.Resolutions {
		c, ok := conflicts[paths{
			theirs: r.OldPath,
			ours:   r.NewPath,
		}]
		if !ok {
			// Note: this emulates the Ruby error that occurs when
			// there are no conflicts for a resolution
			return errors.New("NoMethodError: undefined method `resolve_lines' for nil:NilClass")
		}

		switch {
		case c.Our == nil:
			return fmt.Errorf("missing our-part of merge file input for new path %q", r.NewPath)
		case c.Their == nil:
			return fmt.Errorf("missing their-part of merge file input for new path %q", r.NewPath)
		}

		mfr, err := mergeFileResult(odb, c)
		if err != nil {
			return fmt.Errorf("merge file result for %q: %w", r.NewPath, err)
		}

		if r.Content != "" && bytes.Equal([]byte(r.Content), mfr.Contents) {
			return fmt.Errorf("Resolved content has no changes for file %s", r.NewPath) //nolint
		}

		ancestorPath := c.Our.Path
		if c.Ancestor != nil {
			ancestorPath = c.Ancestor.Path
		}

		conflictFile, err := conflict.Parse(
			bytes.NewReader(mfr.Contents),
			c.Our.Path,
			c.Their.Path,
			ancestorPath,
		)
		if err != nil {
			return fmt.Errorf("parse conflict for %q: %w", c.Ancestor.Path, err)
		}

		resolvedBlob, err := conflictFile.Resolve(r)
		if err != nil {
			return err // do not decorate this error to satisfy old test
		}

		resolvedBlobOID, err := odb.Write(resolvedBlob, git.ObjectBlob)
		if err != nil {
			return fmt.Errorf("write object for %q: %w", c.Ancestor.Path, err)
		}

		ourResolvedEntry := *c.Our // copy by value
		ourResolvedEntry.Id = resolvedBlobOID
		if err := index.Add(&ourResolvedEntry); err != nil {
			return fmt.Errorf("add index for %q: %w", c.Ancestor.Path, err)
		}

		if err := index.RemoveConflict(ourResolvedEntry.Path); err != nil {
			return fmt.Errorf("remove conflict from index for %q: %w", c.Ancestor.Path, err)
		}
	}

	if index.HasConflicts() {
		ci, err := index.ConflictIterator()
		if err != nil {
			return fmt.Errorf("iterating unresolved conflicts: %w", err)
		}

		var conflictPaths []string
		for {
			c, err := ci.Next()
			if git.IsErrorCode(err, git.ErrIterOver) {
				break
			}
			if err != nil {
				return fmt.Errorf("next unresolved conflict: %w", err)
			}
			conflictPaths = append(conflictPaths, c.Ancestor.Path)
		}

		return fmt.Errorf("Missing resolutions for the following files: %s", strings.Join(conflictPaths, ", ")) //nolint
	}

	tree, err := index.WriteTreeTo(repo)
	if err != nil {
		return fmt.Errorf("write tree to repo: %w", err)
	}

	signature := git2go.NewSignature(request.AuthorName, request.AuthorMail, request.AuthorDate)
	committer := &git.Signature{
		Name:  signature.Name,
		Email: signature.Email,
		When:  request.AuthorDate,
	}

	commit, err := repo.CreateCommitFromIds("", committer, committer, request.Message, tree, ours.Id(), theirs.Id())
	if err != nil {
		return fmt.Errorf("could not create resolve conflict commit: %w", err)
	}

	response := git2go.ResolveResult{
		git2go.MergeResult{
			CommitID: commit.String(),
		},
	}

	return gob.NewEncoder(w).Encode(response)
}

func mergeFileResult(odb *git.Odb, c git.IndexConflict) (*git.MergeFileResult, error) {
	var ancestorMFI, ourMFI, theirMFI git.MergeFileInput

	for _, part := range []struct {
		name  string
		entry *git.IndexEntry
		mfi   *git.MergeFileInput
	}{
		{name: "ancestor", entry: c.Ancestor, mfi: &ancestorMFI},
		{name: "our", entry: c.Our, mfi: &ourMFI},
		{name: "their", entry: c.Their, mfi: &theirMFI},
	} {
		if part.entry == nil {
			continue
		}

		blob, err := odb.Read(part.entry.Id)
		if err != nil {
			return nil, err
		}

		part.mfi.Path = part.entry.Path
		part.mfi.Mode = uint(part.entry.Mode)
		part.mfi.Contents = blob.Data()
	}

	mfr, err := git.MergeFile(ancestorMFI, ourMFI, theirMFI, nil)
	if err != nil {
		return nil, err
	}

	return mfr, nil
}
