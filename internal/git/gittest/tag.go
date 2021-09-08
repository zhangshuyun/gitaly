package gittest

import (
	"bytes"
	"fmt"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
)

// WriteTagOpts holds extra options for WriteTag.
type WriteTagOpts struct {
	Message string
	Force   bool
}

// WriteTag writes a new tag into the repository. This function either returns the tag ID in case
// an annotated tag was created, or otherwise the target object ID when a lightweight tag was
// created.
func WriteTag(t testing.TB, cfg config.Cfg, repoPath, tagName, targetID string, opts *WriteTagOpts) string {
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

	args := []string{
		"-C", repoPath,
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
