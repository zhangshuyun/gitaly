package git2go

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
)

var (
	// ErrInvalidArgument is returned in case the merge arguments are invalid.
	ErrInvalidArgument = errors.New("invalid parameters")
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
}

// MergeResult contains results from a merge.
type MergeResult struct {
	// CommitID is the object ID of the generated merge commit.
	CommitID string `json:"commit_id"`
}

func serialize(v interface{}) (string, error) {
	marshalled, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(marshalled), nil
}

func deserialize(serialized string, v interface{}) error {
	base64Decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(serialized))
	jsonDecoder := json.NewDecoder(base64Decoder)
	return jsonDecoder.Decode(v)
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

// Serialize serializes the merge response into its JSON representation and encodes it with base64.
func (m MergeResult) Serialize() (string, error) {
	return serialize(m)
}

// Merge performs a merge via gitaly-git2go.
func (m MergeCommand) Run(ctx context.Context, cfg config.Cfg) (MergeResult, error) {
	if err := m.verify(); err != nil {
		return MergeResult{}, fmt.Errorf("merge: %w: %s", ErrInvalidArgument, err.Error())
	}

	serialized, err := serialize(m)
	if err != nil {
		return MergeResult{}, err
	}

	stdout, err := run(ctx, cfg, "merge", serialized)
	if err != nil {
		return MergeResult{}, err
	}

	var response MergeResult
	if err := deserialize(stdout, &response); err != nil {
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

func run(ctx context.Context, cfg config.Cfg, subcommand string, arg string) (string, error) {
	binary := path.Join(cfg.BinDir, "gitaly-git2go")

	var stderr, stdout bytes.Buffer
	cmd, err := command.New(ctx, exec.Command(binary, subcommand, "-request", arg), nil, &stdout, &stderr)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("%s", stderr.String())
		}
		return "", err
	}

	return stdout.String(), nil
}
