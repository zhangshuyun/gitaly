package lstree

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
)

var (
	// ErrNotExist indicates that the requested tree does not exist, either because the revision
	// is invalid or because the path is not valid.
	ErrNotExist = errors.New("invalid object name")
	// ErrNotTreeish indicates that the requested revision or path does not resolve to a tree
	// object.
	ErrNotTreeish = errors.New("not treeish")
)

// ListEntriesConfig is configuration that can be passed to ListEntries.
type ListEntriesConfig struct {
	// Recursive indicates whether the listing shall be recursive or not.
	Recursive bool
	// RelativePath is the relative path at which listing of entries should be started.
	RelativePath string
}

// ListEntries lists tree entries for the given treeish. By default, this will do a non-recursive
// listing starting from the root of the given treeish. This behaviour can be changed by passing a
// config.
func ListEntries(
	ctx context.Context,
	repo *localrepo.Repo,
	treeish git.Revision,
	cfg *ListEntriesConfig,
) ([]*Entry, error) {
	if cfg == nil {
		cfg = &ListEntriesConfig{}
	}

	flags := []git.Option{git.Flag{Name: "-z"}}
	if cfg.Recursive {
		flags = append(flags,
			git.Flag{Name: "-r"},
			git.Flag{Name: "-t"},
		)
	}

	var stderr bytes.Buffer
	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "ls-tree",
		Args:  []string{fmt.Sprintf("%s:%s", treeish, cfg.RelativePath)},
		Flags: flags,
	}, git.WithStderr(&stderr))
	if err != nil {
		return nil, fmt.Errorf("spawning git-ls-tree: %w", err)
	}

	parser := NewParser(cmd)
	var entries []*Entry
	for {
		entry, err := parser.NextEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("parsing tree entry: %w", err)
		}

		entries = append(entries, entry)
	}

	if err := cmd.Wait(); err != nil {
		errorMessage := stderr.String()
		if strings.HasPrefix(errorMessage, "fatal: not a tree object") {
			return nil, ErrNotTreeish
		} else if strings.HasPrefix(errorMessage, "fatal: Not a valid object name") {
			return nil, ErrNotExist
		}

		return nil, fmt.Errorf("waiting for git-ls-tree: %w, stderr: %q", err, errorMessage)
	}

	return entries, nil
}
