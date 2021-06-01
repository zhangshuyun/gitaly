package git2go

import (
	"context"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
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
	Repository string `json:"repository"`

	// AuthorName is the author name of submodule commit.
	AuthorName string `json:"author_name"`
	// AuthorMail is the author mail of submodule commit.
	AuthorMail string `json:"author_mail"`
	// AuthorDate is the auithor date of submodule commit.
	AuthorDate time.Time `json:"author_date"`
	// Message is the message to be used for the submodule commit.
	Message string `json:"message"`

	// CommitSHA is where the submodule should point
	CommitSHA string `json:"commit_sha"`
	// Submodule is the actual submodule string to commit to the tree
	Submodule string `json:"submodule"`
	// Branch where to commit submodule update
	Branch string `json:"branch"`
}

// SubmoduleResult contains results from a committing a submodule update
type SubmoduleResult struct {
	// CommitID is the object ID of the generated submodule commit.
	CommitID string `json:"commit_id"`
}

// SubmoduleCommandFromSerialized deserializes the submodule request from its JSON representation encoded with base64.
func SubmoduleCommandFromSerialized(serialized string) (SubmoduleCommand, error) {
	var request SubmoduleCommand
	if err := deserialize(serialized, &request); err != nil {
		return SubmoduleCommand{}, err
	}

	if err := request.verify(); err != nil {
		return SubmoduleCommand{}, fmt.Errorf("submodule: %w", err)
	}

	return request, nil
}

// SerializeTo serializes the submodule result and writes it into the writer.
func (s SubmoduleResult) SerializeTo(w io.Writer) error {
	return serializeTo(w, s)
}

// Run attempts to commit the request submodule change
func (s SubmoduleCommand) Run(ctx context.Context, cfg config.Cfg) (SubmoduleResult, error) {
	if err := s.verify(); err != nil {
		return SubmoduleResult{}, fmt.Errorf("submodule: %w", err)
	}

	serialized, err := serialize(s)
	if err != nil {
		return SubmoduleResult{}, err
	}

	stdout, err := run(ctx, binaryPathFromCfg(cfg), nil, "submodule", "-request", serialized)
	if err != nil {
		return SubmoduleResult{}, err
	}

	var response SubmoduleResult
	if err := deserialize(stdout.String(), &response); err != nil {
		return SubmoduleResult{}, err
	}

	return response, nil
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
