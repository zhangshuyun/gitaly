// +build static,system_libgit2

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	git "github.com/libgit2/git2go/v30"
	"gitlab.com/gitlab-org/gitaly/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

type resolveSubcommand struct {
	request string
}

func (cmd *resolveSubcommand) Flags() *flag.FlagSet {
	flags := flag.NewFlagSet("resolve", flag.ExitOnError)
	flags.StringVar(&cmd.request, "request", "", "git2go.MergeCommand")
	return flags
}

func (cmd resolveSubcommand) Run() error {
	request, err := git2go.ResolveCommandFromSerialized(cmd.request)
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

	ci, err := index.ConflictIterator()
	if err != nil {
		return err
	}

	type paths struct {
		theirs, ours string
	}
	conflicts := map[paths]git.IndexConflict{}

	for c, err := ci.Next(); err != nil; c, err = ci.Next() {
		if c.Our.Path == "" || c.Their.Path == "" {
			return errors.New("conflict side missing")
		}

		k := paths{
			theirs: c.Their.Path,
			ours:   c.Our.Path,
		}
		conflicts[k] = c
	}

	for _, r := range request.Resolutions {
		c, ok := conflicts[paths{
			theirs: r.OldPath,
			ours:   r.NewPath,
		}]
		if !ok {
			continue
		}

		odb, err := repo.Odb()
		if err != nil {
			return err
		}

		mfr, err := mergeFileResult(odb, c)
		if err != nil {
			return err
		}

		f, err := conflict.Parse(
			bytes.NewReader(mfr.Contents),
			c.Our.Path,
			c.Their.Path,
			c.Ancestor.Path,
		)
		if err != nil {
			return err
		}

		resolvedBlob, err := f.Resolve(r)
		if err != nil {
			return err
		}

		resolvedBlobOID, err := odb.Write(resolvedBlob, git.ObjectBlob)
		if err != nil {
			return err
		}

		ourResolvedEntry := *c.Our // copy by value
		ourResolvedEntry.Id = resolvedBlobOID
		if err := index.Add(&ourResolvedEntry); err != nil {
			return err
		}

		// TODO: verify we need to remove conflict
		if err := index.RemoveConflict(ourResolvedEntry.Path); err != nil {
			return err
		}
	}

	if index.HasConflicts() {
		ci, err := index.ConflictIterator()
		if err != nil {
			return err
		}

		var conflictPaths []string
		for {
			c, err := ci.Next()
			if err != nil {
				return err
			}
			conflictPaths = append(conflictPaths, c.Ancestor.Path)
		}

		return fmt.Errorf(
			"Missing resolutions for the following files: %s",
			strings.Join(conflictPaths, ", "),
		)
	}

	return nil
}

func mergeFileResult(odb *git.Odb, c git.IndexConflict) (*git.MergeFileResult, error) {
	ancestorBlob, err := odb.Read(c.Ancestor.Id)
	if err != nil {
		return nil, err
	}

	ourBlob, err := odb.Read(c.Our.Id)
	if err != nil {
		return nil, err
	}

	theirBlob, err := odb.Read(c.Their.Id)
	if err != nil {
		return nil, err
	}

	mfr, err := git.MergeFile(
		git.MergeFileInput{
			Path:     c.Ancestor.Path,
			Mode:     uint(c.Ancestor.Mode),
			Contents: ancestorBlob.Data(),
		},
		git.MergeFileInput{
			Path:     c.Our.Path,
			Mode:     uint(c.Our.Mode),
			Contents: ourBlob.Data(),
		},
		git.MergeFileInput{
			Path:     c.Their.Path,
			Mode:     uint(c.Their.Mode),
			Contents: theirBlob.Data(),
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return mfr, nil
}
