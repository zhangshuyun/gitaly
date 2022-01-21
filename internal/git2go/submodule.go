package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
)

// Error strings present in the legacy Ruby implementation
const (
	LegacyErrPrefixInvalidBranch        = "Invalid branch"
	LegacyErrPrefixInvalidSubmodulePath = "Invalid submodule path"
	LegacyErrPrefixFailedCommit         = "Failed to create commit"
)

// SubmoduleCommand instructs how to commit a submodule update to a repo
type SubmoduleCommand struct {
	// Repository is the path to commit the submodule change
	Repository string

	// AuthorName is the author name of submodule commit.
	AuthorName string
	// AuthorMail is the author mail of submodule commit.
	AuthorMail string
	// AuthorDate is the auithor date of submodule commit.
	AuthorDate time.Time
	// Message is the message to be used for the submodule commit.
	Message string

	// CommitSHA is where the submodule should point
	CommitSHA string
	// Submodule is the actual submodule string to commit to the tree
	Submodule string
	// Branch where to commit submodule update
	Branch string
}

// SubmoduleResult contains results from a committing a submodule update
type SubmoduleResult struct {
	// CommitID is the object ID of the generated submodule commit.
	CommitID string
}

// Submodule attempts to commit the request submodule change
func (b Executor) Submodule(ctx context.Context, repo repository.GitRepo, s SubmoduleCommand) (SubmoduleResult, error) {
	if err := s.verify(); err != nil {
		return SubmoduleResult{}, fmt.Errorf("submodule: %w", err)
	}

	input := &bytes.Buffer{}
	const cmd = "submodule"
	if err := gob.NewEncoder(input).Encode(s); err != nil {
		return SubmoduleResult{}, fmt.Errorf("%s: %w", cmd, err)
	}

	// Ideally we would use `b.runWithGob` here to avoid the gob encoding
	// boilerplate but it is not possible here because `runWithGob` adds error
	// prefixes and the `LegacyErrPrefix*` errors must match exactly.
	stdout, err := b.run(ctx, repo, input, cmd)
	if err != nil {
		return SubmoduleResult{}, err
	}

	var result Result
	if err := gob.NewDecoder(stdout).Decode(&result); err != nil {
		return SubmoduleResult{}, fmt.Errorf("%s: %w", cmd, err)
	}

	if result.Err != nil {
		return SubmoduleResult{}, result.Err
	}

	return SubmoduleResult{
		CommitID: result.CommitID,
	}, nil
}

func (s SubmoduleCommand) verify() (err error) {
	if s.Repository == "" {
		return InvalidArgumentError("missing repository")
	}
	if s.AuthorName == "" {
		return InvalidArgumentError("missing author name")
	}
	if s.AuthorMail == "" {
		return InvalidArgumentError("missing author mail")
	}
	if s.CommitSHA == "" {
		return InvalidArgumentError("missing commit SHA")
	}
	if s.Branch == "" {
		return InvalidArgumentError("missing branch name")
	}
	if s.Submodule == "" {
		return InvalidArgumentError("missing submodule")
	}
	return nil
}
