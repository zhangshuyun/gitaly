package gittest

import (
	"bytes"
	"fmt"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
)

// CreateTagOpts holds extra options for CreateTag.
type CreateTagOpts struct {
	Message string
	Force   bool
}

// CreateTag creates a new tag.
func CreateTag(t testing.TB, cfg config.Cfg, repoPath, tagName, targetID string, opts *CreateTagOpts) string {
	var message string
	force := false

	if opts != nil {
		if opts.Message != "" {
			message = opts.Message
		}
		force = opts.Force
	}

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// message can be very large, passing it directly in args would blow things up!
	stdin := bytes.NewBufferString(message)

	args := []string{"-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"tag",
	}

	if force {
		args = append(args, "-f")
	}

	if message != "" {
		args = append(args, "-F", "-")
	}
	args = append(args, tagName, targetID)

	ExecStream(t, cfg, stdin, args...)

	tagID := Exec(t, cfg, "-C", repoPath, "show-ref", "-s", tagName)
	return text.ChompBytes(tagID)
}
