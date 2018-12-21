package testhelper

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// CreateTagOpts holds extra options for CreateTag.
type CreateTagOpts struct {
	Message string
}

// CreateTag creates a new tag.
func CreateTag(t *testing.T, repoPath, tagName, targetID string, opts *CreateTagOpts) string {
	var message string

	if opts != nil {
		if opts.Message != "" {
			message = opts.Message
		}
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
	if message != "" {
		args = append(args, "-F", "-")
	}
	args = append(args, tagName, targetID)

	MustRunCommand(t, stdin, "git", args...)

	tagID := MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", tagName)
	return strings.TrimSpace(string(tagID))
}

// GetTagDate gets the tag date in a timestamp form
func GetTagDate(t *testing.T, repoPath, tagName string) (int64, error) {
	tagInfoLines := strings.Split(string(MustRunCommand(t, nil, "git", "-C", repoPath, "show", "--date=unix", tagName)), "\n")
	timestampString := strings.TrimSpace(strings.SplitN(tagInfoLines[2], "Date:", 2)[1])
	timestamp, err := strconv.ParseInt(strings.TrimSpace(string(timestampString)), 10, 64)
	if err != nil {
		return 0, nil
	}
	return timestamp, nil
}
