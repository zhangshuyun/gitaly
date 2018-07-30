package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/command"
)

// FallbackTimeValue is the value returned by `SafeTimeParse` in case it
// encounters a parse error. It's the maximum time value possible in golang.
// See https://gitlab.com/gitlab-org/gitaly/issues/556#note_40289573
var FallbackTimeValue = time.Unix(1<<63-62135596801, 999999999)

// ValidateRevision checks if a revision looks valid
func ValidateRevision(revision []byte) error {
	if len(revision) == 0 {
		return fmt.Errorf("empty revision")
	}
	if bytes.HasPrefix(revision, []byte("-")) {
		return fmt.Errorf("revision can't start with '-'")
	}
	if bytes.Contains(revision, []byte(" ")) {
		return fmt.Errorf("revision can't contain whitespace")
	}
	if bytes.Contains(revision, []byte("\x00")) {
		return fmt.Errorf("revision can't contain NUL")
	}
	if bytes.Contains(revision, []byte(":")) {
		return fmt.Errorf("revision can't contain ':'")
	}
	return nil
}

// Version returns the used git version.
func Version() (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	cmd, err := command.New(ctx, exec.Command(command.GitPath(), "version"), nil, &buf, nil)
	if err != nil {
		return "", err
	}

	if err = cmd.Wait(); err != nil {
		return "", err
	}

	out := strings.Trim(buf.String(), " \n")
	ver := strings.SplitN(out, " ", 3)
	if len(ver) != 3 {
		return "", fmt.Errorf("invalid version format: %q", buf.String())
	}

	return ver[2], nil
}

// BuildGitOptions helps to generate options to the git command.
// If gitOpts is not empty then its values are passed as part of
// the "-c" option of the git command, the other values are passed along with the subcommand.
func BuildGitOptions(gitOpts []string, otherOpts ...string) []string {
	args := []string{}

	if len(gitOpts) > 0 {
		args = append([]string{"-c"}, gitOpts...)
	}

	return append(args, otherOpts...)
}
