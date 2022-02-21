package housekeeping

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

// WriteCommitGraph updates the commit-graph in the given repository. The commit-graph is updated
// incrementally, except in the case where it doesn't exist yet or in case it is detected that the
// commit-graph is missing bloom filters.
func WriteCommitGraph(ctx context.Context, repo *localrepo.Repo) error {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}

	missingBloomFilters := true
	if _, err := os.Stat(filepath.Join(repoPath, stats.CommitGraphRelPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return helper.ErrInternal(fmt.Errorf("remove commit graph file: %w", err))
		}

		// objects/info/commit-graph file doesn't exists
		// check if commit-graph chain exists and includes Bloom filters
		if missingBloomFilters, err = stats.IsMissingBloomFilters(repoPath); err != nil {
			return helper.ErrInternal(fmt.Errorf("should remove commit graph chain: %w", err))
		}
	}

	flags := []git.Option{
		git.Flag{Name: "--reachable"},
		git.Flag{Name: "--changed-paths"}, // enables Bloom filters
		git.ValueFlag{Name: "--size-multiple", Value: "4"},
	}

	if missingBloomFilters {
		// if commit graph doesn't use Bloom filters we instruct operation to replace
		// existent commit graph with the new one
		// https://git-scm.com/docs/git-commit-graph#Documentation/git-commit-graph.txt-emwriteem
		flags = append(flags, git.Flag{Name: "--split=replace"})
	} else {
		flags = append(flags, git.Flag{Name: "--split"})
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "commit-graph",
		Action: "write",
		Flags:  flags,
	}, git.WithStderr(&stderr)); err != nil {
		return helper.ErrInternalf("writing commit-graph: %s: %v", err, stderr.String())
	}

	return nil
}
