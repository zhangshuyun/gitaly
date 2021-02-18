package git2go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

const (
	// MergeRecursionLimit limits how many virtual merge bases are computed
	// in a recursive merge.
	MergeRecursionLimit = 20
)

// MergeCommand contains parameters to perform a merge.
type MergeCommand struct {
	// Repository is the path to execute merge in.
	Repository string `json:"repository"`
	// AuthorName is the author name of merge commit.
	AuthorName string `json:"author_name"`
	// AuthorMail is the author mail of merge commit.
	AuthorMail string `json:"author_mail"`
	// AuthorDate is the auithor date of merge commit.
	AuthorDate time.Time `json:"author_date"`
	// Message is the message to be used for the merge commit.
	Message string `json:"message"`
	// Ours is the commit that is to be merged into theirs.
	Ours string `json:"ours"`
	// Theirs is the commit into which ours is to be merged.
	Theirs string `json:"theirs"`
	// AllowConflicts controls whether conflicts are allowed. If they are,
	// then conflicts will be committed as part of the result.
	AllowConflicts bool `json:"allow_conflicts"`
}

// MergeResult contains results from a merge.
type MergeResult struct {
	// CommitID is the object ID of the generated merge commit.
	CommitID string `json:"commit_id"`
}

// MergeCommandFromSerialized deserializes the merge request from its JSON representation encoded with base64.
func MergeCommandFromSerialized(serialized string) (MergeCommand, error) {
	var request MergeCommand
	if err := deserialize(serialized, &request); err != nil {
		return MergeCommand{}, err
	}

	if err := request.verify(); err != nil {
		return MergeCommand{}, fmt.Errorf("merge: %w: %s", ErrInvalidArgument, err.Error())
	}

	return request, nil
}

// SerializeTo serializes the merge result and writes it into the writer.
func (m MergeResult) SerializeTo(w io.Writer) error {
	return serializeTo(w, m)
}

// Run performs a merge via gitaly-git2go.
func (m MergeCommand) Run(ctx context.Context, cfg config.Cfg) (MergeResult, error) {
	if err := m.verify(); err != nil {
		return MergeResult{}, fmt.Errorf("merge: %w: %s", ErrInvalidArgument, err.Error())
	}

	serialized, err := serialize(m)
	if err != nil {
		return MergeResult{}, err
	}

	stdout, err := run(ctx, binaryPathFromCfg(cfg), nil, "merge", "-request", serialized)
	if err != nil {
		return MergeResult{}, err
	}

	var response MergeResult
	if err := deserialize(stdout.String(), &response); err != nil {
		return MergeResult{}, err
	}

	return response, nil
}

func (m MergeCommand) verify() error {
	if m.Repository == "" {
		return errors.New("missing repository")
	}
	if m.AuthorName == "" {
		return errors.New("missing author name")
	}
	if m.AuthorMail == "" {
		return errors.New("missing author mail")
	}
	if m.Message == "" {
		return errors.New("missing message")
	}
	if m.Ours == "" {
		return errors.New("missing ours")
	}
	if m.Theirs == "" {
		return errors.New("missing theirs")
	}
	return nil
}
