package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

const (
	// A standard terminal window is (at least) 80 characters wide.
	terminalWidth                = 80
	gitRemoteMessagePrefixLength = len("remote: ")
	terminalMessagePadding       = 2

	// Git prefixes remote messages with "remote: ", so this width is subtracted
	// from the width available to us.
	maxMessageWidth = terminalWidth - gitRemoteMessagePrefixLength

	// Our centered text shouldn't start or end right at the edge of the window,
	// so we add some horizontal padding: 2 chars on either side.
	maxMessageTextWidth = maxMessageWidth - 2*terminalMessagePadding
)

func getEnvVar(key string, vars []string) string {
	for _, varPair := range vars {
		kv := strings.SplitN(varPair, "=", 2)
		if kv[0] == key {
			return kv[1]
		}
	}

	return ""
}

func printMessages(messages []PostReceiveMessage, w io.Writer) error {
	for _, message := range messages {
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}

		switch message.Type {
		case "basic":
			if _, err := w.Write([]byte(message.Message)); err != nil {
				return err
			}
		case "alert":
			if err := printAlert(message, w); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid message type: %v", message.Type)
		}

		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
	}

	return nil
}

func centerLine(b []byte) []byte {
	b = bytes.TrimSpace(b)
	linePadding := int(math.Max((float64(maxMessageWidth)-float64(len(b)))/2, 0))
	return append(bytes.Repeat([]byte(" "), linePadding), b...)
}

func printAlert(m PostReceiveMessage, w io.Writer) error {
	if _, err := w.Write(bytes.Repeat([]byte("="), maxMessageWidth)); err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}

	words := strings.Fields(m.Message)

	line := bytes.NewBufferString("")

	for _, word := range words {
		if line.Len()+1+len(word) > maxMessageTextWidth {
			if _, err := w.Write(append(centerLine(line.Bytes()), '\n')); err != nil {
				return err
			}
			line.Reset()
		}

		if _, err := line.WriteString(word + " "); err != nil {
			return err
		}
	}

	if _, err := w.Write(centerLine(line.Bytes())); err != nil {
		return err
	}

	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}

	if _, err := w.Write(bytes.Repeat([]byte("="), maxMessageWidth)); err != nil {
		return err
	}

	return nil
}

func (m *GitLabHookManager) PostReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternalf("reading stdin from request: %w", err)
	}

	primary, err := isPrimary(env)
	if err != nil {
		return helper.ErrInternalf("could not check role: %w", err)
	}

	if !primary {
		return nil
	}

	glID, glRepo := getEnvVar("GL_ID", env), getEnvVar("GL_REPOSITORY", env)

	ok, messages, err := m.gitlabAPI.PostReceive(ctx, glRepo, glID, string(changes), pushOptions...)
	if err != nil {
		return fmt.Errorf("GitLab: %v", err)
	}

	if err := printMessages(messages, stdout); err != nil {
		return fmt.Errorf("error writing messages to stream: %v", err)
	}

	if !ok {
		return errors.New("")
	}

	executor, err := m.newCustomHooksExecutor(repo, "post-receive")
	if err != nil {
		return helper.ErrInternalf("creating custom hooks executor: %v", err)
	}

	if err = executor(
		ctx,
		nil,
		append(env, hooks.GitPushOptions(pushOptions)...),
		bytes.NewReader(changes),
		stdout,
		stderr,
	); err != nil {
		return err
	}

	return nil
}
