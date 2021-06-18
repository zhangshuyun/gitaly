package gittest

import (
	"io"
	"os"
	"os/exec"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// Exec runs a git command and returns the standard output, or fails.
func Exec(t testing.TB, cfg config.Cfg, args ...string) []byte {
	t.Helper()

	return run(t, nil, cfg, args...)
}

// ExecStream runs a git command with an input stream and returns the standard output, or fails.
func ExecStream(t testing.TB, cfg config.Cfg, stream io.Reader, args ...string) []byte {
	t.Helper()

	return run(t, stream, cfg, args...)
}

func run(t testing.TB, stdin io.Reader, cfg config.Cfg, args ...string) []byte {
	t.Helper()

	cmd := exec.Command(cfg.Git.BinPath, args...)
	cmd.Env = os.Environ()
	cmd.Env = append(command.GitEnv, cmd.Env...)
	cmd.Env = append(cmd.Env,
		"GIT_AUTHOR_DATE=1572776879 +0100",
		"GIT_COMMITTER_DATE=1572776879 +0100",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=init.defaultBranch",
		"GIT_CONFIG_VALUE_0=master",
	)

	if stdin != nil {
		cmd.Stdin = stdin
	}

	output, err := cmd.Output()
	if err != nil {
		stderr := err.(*exec.ExitError).Stderr
		t.Log(cfg.Git.BinPath, args)
		t.Logf("%s: %s\n", stderr, output)
		t.Fatal(err)
	}

	return output
}
